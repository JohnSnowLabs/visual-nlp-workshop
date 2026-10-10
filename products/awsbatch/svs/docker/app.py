import os
import logging
import sys
import traceback
import boto3
from botocore.exceptions import ClientError
import tempfile
import shutil
import argparse
import csv
import json
import time
from urllib.parse import urlparse
import tifffile
from johnsnowlabs import nlp
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col
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
    # sparkocr adds its own root handler: without this every line is logged twice
    logger.propagate = False
    return logger


logger = get_logger("deid-batch-job")
CACHE_PRETRAINED_PATH = "/opt/ml"
DEID_MODE = os.environ.get("DEID_MODE", "pipeline").lower()
if DEID_MODE not in ("blanket", "pipeline"):
    raise ValueError(f"DEID_MODE must be 'blanket' or 'pipeline', got {DEID_MODE!r}")
# cpu or gpu, the one the image was built for (see the Dockerfile)
HARDWARE_TARGET = os.environ.get("HARDWARE_TARGET", "cpu").lower()

def start_spark():
    # SPARK_OCR_LICENSE is read directly by nlp.start() from the process
    # environment. AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are only ever
    # used at image build time, by docker/installer.py (via a Docker build
    # secret, see docker/README.md) to install the licensed jars/model --
    # nlp.start() at container runtime doesn't need them. So they're never
    # passed to the running container at all, and boto3 is free to use the
    # Batch job's IAM task role for S3 access without any collision.
    return nlp.start(visual=True, hardware_target=HARDWARE_TARGET)

spark = None

def load_pipeline():
    """
    Load the pipeline of DEID_MODE: blanket, a text detector; pipeline, the de-identification pipeline.
    """
    if DEID_MODE == "pipeline":
        model_path = os.path.join(CACHE_PRETRAINED_PATH, "model")
        if not os.path.isdir(model_path):
            raise RuntimeError("DEID_MODE=pipeline needs the image built with MODEL_TO_LOAD set")
        return PipelineModel.load(model_path)

    text_detector = ImageTextDetectorCraft().load(os.path.join(CACHE_PRETRAINED_PATH, "image_text_detector_mem_opt")) \
    .setInputCol("image_raw") \
    .setOutputCol("coordinates") \
    .setScoreThreshold(0.7) \
    .setLinkThreshold(0.5) \
    .setWithRefiner(True) \
    .setTextThreshold(0.4) \
    .setSizeThreshold(-1) \
    .setUseGPU(HARDWARE_TARGET == "gpu") \
    .setWidth(0) \
    .setHeight(0)

    return PipelineModel(stages=[text_detector])

def count_tiles(svs_path, boxes=None):
    """Tiles of all pyramid levels, or only those the level-0 `boxes` lie on."""
    total = 0
    with tifffile.TiffFile(svs_path) as tif:
        # pyramid levels: the tiled pages with the base image's shape
        tiled = [p for p in tif.pages if p.is_tiled and len(p.shape) >= 2]
        base = max(tiled, key=lambda p: p.shape[0] * p.shape[1])
        ratio = base.shape[1] / base.shape[0]
        levels = [p for p in tiled if abs(p.shape[1] / p.shape[0] - ratio) < 0.03 * ratio]
        for page in levels:
            height, width = page.shape[:2]
            tw, th = page.tilewidth, page.tilelength
            if boxes is None:
                total += -(-width // tw) * -(-height // th)
                continue
            fx, fy = width / base.shape[1], height / base.shape[0]
            touched = set()
            for x1, y1, x2, y2 in boxes:
                for r in range(max(0, int(y1 * fy)) // th, min(height - 1, int(y2 * fy)) // th + 1):
                    for c in range(max(0, int(x1 * fx)) // tw, min(width - 1, int(x2 * fx)) // tw + 1):
                        touched.add((r, c))
            total += len(touched)
    return total


def process_file(pipeline, input_file, filename, output_folder):
    """Run the de-id pipeline on a single local file and return the local
    path to its output file inside output_folder, and its tile counts."""
    from sparkocr.utils.svs.phi_cleaning import remove_phi
    from sparkocr.utils.svs.deidentify import svs_to_text_images, redact_phi_in_text_images

    cleaned_header_tmp = tempfile.mkdtemp(dir=output_folder, prefix="cleaned_header_")
    remove_phi(input_file, cleaned_header_tmp, verbose=True, rename=False)
    fully_qualified_filename = os.path.join(cleaned_header_tmp, filename)

    text_images_tmp = tempfile.mkdtemp(dir=output_folder, prefix="text_images_")
    images = svs_to_text_images(fully_qualified_filename, text_images_tmp)
    to_image = BinaryToImage().setInputCol("content").setOutputCol("image_raw").setImageType(ImageType.TYPE_BYTE_GRAY)
    result = pipeline.transform(to_image.transform(spark.read.format("binaryFile").load(images).repartition(4))).cache()
    try:
        # all the text the pipeline found, for the counts: pipeline mode redacts only the PHI in it
        text = result if DEID_MODE == "blanket" else result.select("path", col("text_regions").alias("coordinates"))
        text_boxes = redact_phi_in_text_images(fully_qualified_filename, text, text_images_tmp, dry_run=True)[filename]

        create_new_svs_file = os.environ.get("CREATE_NEW_SVS_FILE", "false").lower() == "true"
        output_svs_path = os.path.join(output_folder, filename) if create_new_svs_file else None
        boxes = redact_phi_in_text_images(fully_qualified_filename, result, text_images_tmp, output_svs_path,
                                          create_new_svs_file)[filename]
    finally:
        result.unpersist()

    tiles = {
        "tiles": count_tiles(fully_qualified_filename),
        "tiles_with_text": count_tiles(fully_qualified_filename, text_boxes),
        "tiles_redacted": count_tiles(fully_qualified_filename, boxes),
    }
    if create_new_svs_file:
        fully_qualified_filename = output_svs_path

    return fully_qualified_filename, tiles


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


METRICS = ["file_name", "file_size_bytes", "tiles", "tiles_with_text", "tiles_redacted",
           "processing_seconds", "status"]


def write_metrics(s3, output_s3, rows):
    """One csv per job (and attempt) in the output folder, rewritten after every file."""
    job = os.environ.get("AWS_BATCH_JOB_ID", "local").replace(":", "_")
    name = f"metrics_{job}_{os.environ.get('AWS_BATCH_JOB_ATTEMPT', '1')}.csv"
    with tempfile.NamedTemporaryFile("w", newline="", suffix=".csv") as f:
        writer = csv.DictWriter(f, fieldnames=METRICS)
        writer.writeheader()
        writer.writerows(rows)
        f.flush()
        bucket, key = marker_key(output_s3, name)
        s3.upload_file(f.name, bucket, key)


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

    pipeline = load_pipeline()

    with tempfile.TemporaryDirectory() as tmp_input_folder, tempfile.TemporaryDirectory() as tmp_output_folder:
        failed_files = []
        metrics = []

        for key in keys:
            filename = os.path.basename(key)
            if has_marker(s3, output_s3, f"_SUCCESS_{filename}"):
                logger.info("Skipping %s, already de-identified", filename)
                continue
            per_file_folder = tempfile.mkdtemp(dir=tmp_input_folder, prefix='tmp_input')
            per_file_output = tempfile.mkdtemp(dir=tmp_output_folder, prefix='tmp_output')
            local_path = os.path.join(per_file_folder, filename)
            row = {"file_name": filename, "status": "failed"}
            start = time.time()

            try:
                logger.info("Downloading %s...", key)
                s3.download_file(in_bucket, key, local_path)
                row["file_size_bytes"] = os.path.getsize(local_path)

                logger.info("Processing %s...", filename)
                output_local, tiles = process_file(pipeline, per_file_folder, filename, per_file_output)
                row.update(tiles)
                out_key = os.path.join(out_prefix, filename)

                logger.info("Uploading to %s...", out_key)
                s3.upload_file(output_local, out_bucket, out_key)
                write_marker(s3, output_s3, f"_SUCCESS_{filename}")
                failure_bucket, failure_key = marker_key(output_s3, f"_FAILURE_{filename}")
                s3.delete_object(Bucket=failure_bucket, Key=failure_key)
                row["status"] = "success"
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

            row["processing_seconds"] = round(time.time() - start, 1)
            logger.info("Metrics %s: size=%s bytes, tiles=%s, tiles with text=%s, tiles redacted=%s, time=%ss, %s",
                        filename, row.get("file_size_bytes"), row.get("tiles"), row.get("tiles_with_text"),
                        row.get("tiles_redacted"), row["processing_seconds"], row["status"])
            metrics.append(row)
            try:
                write_metrics(s3, output_s3, metrics)
            except Exception:
                logger.exception("Failed to write the metrics csv")

        if failed_files:
            logger.error(
                "%d of %d file(s) failed: %s",
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

    # failed files keep their _FAILURE_ marker; the job fails only if none was processed
    if failed_files and len(failed_files) == len(keys):
        sys.exit(1)
