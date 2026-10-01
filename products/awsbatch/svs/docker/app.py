import os
import logging
import sys
import traceback
import boto3
from botocore.exceptions import ClientError
import tempfile
import shutil
import argparse
import json
from urllib.parse import urlparse
from johnsnowlabs import nlp
from pyspark.ml import PipelineModel
from sparkocr.enums import *
from sparkocr.transformers import *

def show_boto3_credentials(mask=True):
    session = boto3.Session()
    creds = session.get_credentials()

    if creds is None:
        print("No credentials found by boto3.")
        return

    # Resolve to a frozen, concrete set of values (this also triggers
    # refresh logic for role-assumed / instance-profile credentials).
    frozen = creds.get_frozen_credentials()

    def mask_value(v, keep=4):
        if not v:
            return v
        return v if not mask else f"{v[:keep]}...{v[-keep:]} (len={len(v)})"

    print("Access Key ID:     ", mask_value(frozen.access_key))
    print("Secret Access Key: ", mask_value(frozen.secret_key))
    print("Session Token:     ", mask_value(frozen.token) if frozen.token else None)
    print("Credential method: ", creds.method)  # e.g. 'env', 'shared-credentials-file', 'iam-role', 'sts-assume-role'
    print("Region:            ", session.region_name)

    # Confirm identity + check expiry via STS (also validates the token works)
    try:
        sts = session.client("sts")
        identity = sts.get_caller_identity()
        print("Account:           ", identity["Account"])
        print("ARN:               ", identity["Arn"])
    except Exception as e:
        print("get_caller_identity failed:", e)
        

def get_logger(logger_name):
    log_level = os.environ.get("LOG_LEVEL", "ERROR").upper()
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    handler.setFormatter(
        logging.Formatter("%(name)s [%(asctime)s] [%(levelname)s] %(message)s")
    )
    logger.addHandler(handler)
    return logger


logger = get_logger("deid-batch-job")
CACHE_PRETRAINED_PATH = "/opt/ml"
DEID_MODE = os.environ.get("DEID_MODE", "pipeline").lower()
if DEID_MODE not in ("blanket", "pipeline"):
    raise ValueError(f"DEID_MODE must be 'blanket' or 'pipeline', got {DEID_MODE!r}")

def start_spark():
    # SPARK_OCR_LICENSE is read directly by nlp.start() from the process
    # environment. AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are only ever
    # used at image build time, by docker/installer.py (via a Docker build
    # secret, see docker/README.md) to install the licensed jars/model --
    # nlp.start() at container runtime doesn't need them. So they're never
    # passed to the running container at all, and boto3 is free to use the
    # Batch job's IAM task role for S3 access without any collision.
    return nlp.start(visual=True)

spark = None

def load_pipeline():
    """
    Load the text detector and, for DEID_MODE=pipeline, the de-identification pipeline.
    """
    text_detector = ImageTextDetectorCraft().load(os.path.join(CACHE_PRETRAINED_PATH, "image_text_detector_mem_opt")) \
    .setScoreThreshold(0.7) \
    .setLinkThreshold(0.5) \
    .setWithRefiner(True) \
    .setTextThreshold(0.4) \
    .setSizeThreshold(-1) \
    .setUseGPU(False) \
    .setWidth(0) \
    .setHeight(0)

    # blanket: every detected text region is redacted; pipeline: only what its NER finds as PHI
    if DEID_MODE == "blanket":
        return text_detector, None
    model_path = os.path.join(CACHE_PRETRAINED_PATH, "model")
    if not os.path.isdir(model_path):
        raise RuntimeError("DEID_MODE=pipeline needs the image built with MODEL_TO_LOAD set")
    return text_detector, PipelineModel.load(model_path)

def process_file(text_detector, deid_pipeline, input_file, filename, output_folder):
    """Run the de-id pipeline on a single local file and return the local
    path to its output file inside output_folder."""
    from sparkocr.utils.svs.phi_cleaning import remove_phi
    from sparkocr.utils.svs.deidentify import detect_phi_boxes, redact_boxes

    cleaned_header_tmp = tempfile.mkdtemp(dir=output_folder, prefix="cleaned_header_")
    remove_phi(input_file, cleaned_header_tmp, verbose=True, rename=False)
    fully_qualified_filename = os.path.join(cleaned_header_tmp, filename)

    boxes = detect_phi_boxes(fully_qualified_filename, DEID_MODE, text_detector, deid_pipeline)
    print(f"number of regions to redact {len(boxes)}")
    if boxes:
        create_new_svs_file = os.environ.get("CREATE_NEW_SVS_FILE", "false").lower() == "true"
        if create_new_svs_file:
            output_svs_path = os.path.join(output_folder, filename)
            redact_boxes(fully_qualified_filename, boxes, output_svs_path, create_new_svs_file=True)
            fully_qualified_filename = output_svs_path
        else:
            redact_boxes(fully_qualified_filename, boxes)

    return fully_qualified_filename


# ---- S3 helpers ----
def parse_s3_uri(uri):
    parsed = urlparse(uri)
    return parsed.netloc, parsed.path.lstrip("/")


def list_s3_files(s3, bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/") and not os.path.basename(key).startswith("_"):
                yield key


def marker_key(output_s3, name):
    out_bucket, out_prefix = parse_s3_uri(output_s3)
    return out_bucket, os.path.join(out_prefix, name) if out_prefix else name


def write_marker(s3, output_s3, name, body=""):
    bucket, key = marker_key(output_s3, name)
    s3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
    logger.info("Wrote marker s3://%s/%s", bucket, key)


def has_marker(s3, output_s3, name):
    bucket, key = marker_key(output_s3, name)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def write_failure_marker(s3, output_s3, error_message, filename=None):
    write_marker(s3, output_s3, f"_FAILURE_{filename}" if filename else "_FAILURE", error_message)


def input_keys(s3):
    """(bucket, keys) to process: this job's manifest (MANIFEST_S3_URI, one per array child),
    or every file under INPUT_S3_URI."""
    manifest_s3 = os.environ.get("MANIFEST_S3_URI")
    if manifest_s3:
        bucket, prefix = parse_s3_uri(manifest_s3)
        index = os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX", "0")
        body = s3.get_object(Bucket=bucket, Key=os.path.join(prefix, f"{index}.json"))["Body"].read()
        manifest = json.loads(body)
        return manifest["bucket"], manifest["keys"]
    in_bucket, in_prefix = parse_s3_uri(os.environ["INPUT_S3_URI"])
    return in_bucket, list(list_s3_files(s3, in_bucket, in_prefix))


# ---- main ----
def process_files(s3, in_bucket, keys, output_s3):
    """De-identify `keys` into output_s3. Each file ends with a _SUCCESS_ or _FAILURE_ marker
    next to its output; files that already have a _SUCCESS_ one are skipped."""
    out_bucket, out_prefix = parse_s3_uri(output_s3)
    if not keys:
        raise ValueError(f"No input files found in s3://{in_bucket}")

    text_detector, deid_pipeline = load_pipeline()

    with tempfile.TemporaryDirectory() as tmp_input_folder, tempfile.TemporaryDirectory() as tmp_output_folder:
        failed_files = []

        for key in keys:
            filename = os.path.basename(key)
            if has_marker(s3, output_s3, f"_SUCCESS_{filename}"):
                logger.info("Skipping %s, already de-identified", filename)
                continue
            per_file_folder = tempfile.mkdtemp(dir=tmp_input_folder, prefix='tmp_input')
            per_file_output = tempfile.mkdtemp(dir=tmp_output_folder, prefix='tmp_output')
            local_path = os.path.join(per_file_folder, filename)

            try:
                logger.info("Downloading %s...", key)
                s3.download_file(in_bucket, key, local_path)

                logger.info("Processing %s...", filename)
                output_local = process_file(text_detector, deid_pipeline, per_file_folder, filename, per_file_output)
                out_key = os.path.join(out_prefix, filename)

                logger.info("Uploading to %s...", out_key)
                s3.upload_file(output_local, out_bucket, out_key)
                write_marker(s3, output_s3, f"_SUCCESS_{filename}")
                failure_bucket, failure_key = marker_key(output_s3, f"_FAILURE_{filename}")
                s3.delete_object(Bucket=failure_bucket, Key=failure_key)
            except Exception:
                error_message = traceback.format_exc()
                logger.error("Failed to process %s:\n%s", filename, error_message)
                failed_files.append(filename)
                try:
                    write_failure_marker(s3, output_s3, error_message, filename=filename)
                except Exception:
                    logger.exception("Failed to write failure marker for %s", filename)
            finally:
                # clean up this file only, keep parent dirs for the next one
                shutil.rmtree(per_file_output, ignore_errors=True)
                shutil.rmtree(per_file_folder, ignore_errors=True)

        if failed_files:
            logger.error(
                "Failed to process %d/%d file(s): %s",
                len(failed_files), len(keys), failed_files,
            )

    logger.info("Done!")
    return failed_files


def get_config():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=False, help="s3://bucket/prefix/")
    parser.add_argument("--output", required=False, help="s3://bucket/prefix/")
    args, _ = parser.parse_known_args()

    if args.input:
        os.environ["INPUT_S3_URI"] = args.input
    output_s3 = args.output or os.environ.get("OUTPUT_S3_URI")

    if not (os.environ.get("INPUT_S3_URI") or os.environ.get("MANIFEST_S3_URI")) or not output_s3:
        raise ValueError(
            "Input/output S3 locations must be provided via --input/--output "
            "or the INPUT_S3_URI (or MANIFEST_S3_URI)/OUTPUT_S3_URI environment variables."
        )
    return output_s3


if __name__ == "__main__":
    output_s3 = get_config()
    s3_client = boto3.client("s3")
    show_boto3_credentials(mask=True)
    spark = start_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        in_bucket, keys = input_keys(s3_client)
        failed_files = process_files(s3_client, in_bucket, keys, output_s3)
    except Exception:
        error_message = traceback.format_exc()
        logger.error(error_message)
        try:
            write_failure_marker(s3_client, output_s3, error_message)
        except Exception:
            logger.exception("Failed to write _FAILURE marker to %s", output_s3)
        sys.exit(1)

    if failed_files:
        sys.exit(1)
