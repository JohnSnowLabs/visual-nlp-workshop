import os
import logging
import sys
import traceback
import boto3
import tempfile
import shutil
import argparse
from urllib.parse import urlparse
from johnsnowlabs import nlp
from pyspark.ml import PipelineModel
from sparkocr.enums import *
from sparkocr.transformers import *
from pyspark.sql.functions import size

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
    Load the Spark OCR de-id pipeline.
    """

    bin_to_image = BinaryToImage() \
    .setInputCol("content") \
    .setOutputCol("image_raw") \
    .setImageType(ImageType.TYPE_BYTE_GRAY) \
    .setKeepInput(False)

    text_detector = ImageTextDetectorCraft().load(os.path.join(CACHE_PRETRAINED_PATH, "image_text_detector_mem_opt")) \
    .setInputCol("image_raw") \
    .setOutputCol("text_regions") \
    .setScoreThreshold(0.7) \
    .setLinkThreshold(0.5) \
    .setWithRefiner(True) \
    .setTextThreshold(0.4) \
    .setSizeThreshold(-1) \
    .setUseGPU(False) \
    .setWidth(640)
    
    img_pipeline = PipelineModel(stages=[bin_to_image, text_detector])

    nlp_pipeline = PipelineModel.load(os.path.join(CACHE_PRETRAINED_PATH, "model"))
    nlp_pipeline.stages = nlp_pipeline.stages[1:]
    return img_pipeline, nlp_pipeline

def process_file(img_p, nlp_p, input_file, filename, output_folder):
    """Run the de-id pipeline on a single local file and return the local
    path to its output file inside output_folder."""
    from sparkocr.utils.svs.phi_cleaning import remove_phi
    from sparkocr.utils.svs.tile_extraction import svs_to_tiles
    from sparkocr.utils.svs.phi_redaction import redact_phi_in_tiles
    import os
    
    cleaned_header_tmp = tempfile.mkdtemp(dir=output_folder, prefix="cleaned_header_")
    remove_phi(input_file, cleaned_header_tmp, verbose=True, rename=False)
    tiles_output_tmp = tempfile.mkdtemp(dir=output_folder, prefix="tiles_output_")
    svs_to_tiles(input_file, tiles_output_tmp, level="auto", thumbnail = True)

    selected_level_paths = []
    # Iterate over each folder inside tiles_output
    for svs_folder in os.listdir(tiles_output_tmp):
        svs_folder_path = os.path.join(tiles_output_tmp, svs_folder)
        if os.path.isdir(svs_folder_path):
            # Look for selected_level_tiles inside each svs folder
            selected_level_folder = os.path.join(svs_folder_path, "selected")
            if os.path.isdir(selected_level_folder):
                selected_level_paths.append(selected_level_folder)
    print(f"selected_level_paths:{selected_level_paths}")
    
    image_df = spark.read.format("binaryFile").load(selected_level_paths)
    print(f"number of images:{image_df.count()}")
    regions_df = img_p.transform(image_df)
    regions_df = regions_df.filter(size(regions_df["text_regions"]) > 0).cache()
    print(f"number of tiles w/text {regions_df.count()}")

    fully_qualified_filename = os.path.join(cleaned_header_tmp, filename)
    if regions_df.count() > 0:
      coords_df = nlp_p.transform(regions_df)
      deid_info = coords_df.select("path", "coordinates").distinct()

      create_new_svs_file = os.environ.get("CREATE_NEW_SVS_FILE", "false").lower() == "true"
      output_svs_path = os.path.join(output_folder, filename) if create_new_svs_file else output_folder

      redact_phi_in_tiles(fully_qualified_filename, deid_info, tiles_output_tmp, output_svs_path=output_svs_path, create_new_svs_file=create_new_svs_file)

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
            if not key.endswith("/") and not key.endswith("_READY"):
                yield key


def write_failure_marker(s3, output_s3, error_message, filename=None):
    out_bucket, out_prefix = parse_s3_uri(output_s3)
    marker_name = f"_FAILURE_{filename}" if filename else "_FAILURE"
    key = os.path.join(out_prefix, marker_name) if out_prefix else marker_name
    s3.put_object(Bucket=out_bucket, Key=key, Body=error_message.encode("utf-8"))
    logger.info("Wrote failure marker to s3://%s/%s", out_bucket, key)


# ---- main ----
def process_folder(s3, input_s3, output_s3):
    in_bucket, in_prefix = parse_s3_uri(input_s3)
    out_bucket, out_prefix = parse_s3_uri(output_s3)

    img_p, nlp_p = load_pipeline()

    with tempfile.TemporaryDirectory() as tmp_input_folder, tempfile.TemporaryDirectory() as tmp_output_folder:
        keys = list(list_s3_files(s3, in_bucket, in_prefix))
        if not keys:
            raise ValueError(f"No input files found under s3://{in_bucket}/{in_prefix}")

        failed_files = []

        for key in keys:
            filename = os.path.basename(key)
            per_file_folder = tempfile.mkdtemp(dir=tmp_input_folder, prefix='tmp_input')
            local_path = os.path.join(per_file_folder, filename)

            try:
                logger.info("Downloading %s...", key)
                s3.download_file(in_bucket, key, local_path)

                logger.info("Processing %s...", filename)
                output_local = process_file(img_p, nlp_p, per_file_folder, filename, tmp_output_folder)
                out_key = os.path.join(out_prefix, filename)

                logger.info("Uploading to %s...", out_key)
                s3.upload_file(output_local, out_bucket, out_key)
            except Exception:
                error_message = traceback.format_exc()
                logger.error("Failed to process %s:\n%s", filename, error_message)
                failed_files.append(filename)
                try:
                    write_failure_marker(s3, output_s3, error_message, filename=filename)
                except Exception:
                    logger.exception("Failed to write failure marker for %s", filename)
            finally:
                # remove the tiles, the copies, and the final result for this file
                shutil.rmtree(tmp_output_folder, ignore_errors=True)
                # remove the local .svs file, whether or not processing got that far
                if os.path.exists(local_path):
                    os.remove(local_path)

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

    input_s3 = args.input or os.environ.get("INPUT_S3_URI")
    output_s3 = args.output or os.environ.get("OUTPUT_S3_URI")

    if not input_s3 or not output_s3:
        raise ValueError(
            "Input/output S3 locations must be provided via --input/--output "
            "or the INPUT_S3_URI/OUTPUT_S3_URI environment variables."
        )
    return input_s3, output_s3


if __name__ == "__main__":
    input_s3, output_s3 = get_config()
    s3_client = boto3.client("s3")
    show_boto3_credentials(mask=True)
    spark = start_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        failed_files = process_folder(s3_client, input_s3, output_s3)
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
