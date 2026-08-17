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
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType


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
    """Load the baked DICOM de-id pipeline: JSL's pretrained
    dicom_deid_full_anonymization (OCR text detection + blanket redaction of
    every detected text region in the image, plus DICOM metadata deid).
    Baked in at image build time by installer.py."""
    return PipelineModel.load(os.path.join(CACHE_PRETRAINED_PATH, "dicom_pipeline"))


def get_name(path):
    return os.path.splitext(os.path.basename(path))[0]


get_name_udf = udf(get_name, StringType())


def process_file(pipeline, input_file, filename, output_folder):
    """Run the de-id pipeline on a single local DICOM file and return the
    local path to its output file inside output_folder."""
    dicom_df = spark.read.format("binaryFile").load(input_file)
    result = pipeline.transform(dicom_df).cache()

    result.withColumn("fileName", get_name_udf(col("path"))) \
        .write \
        .format("binaryFormat") \
        .option("type", "dicom") \
        .option("field", "dicom_cleaned") \
        .option("prefix", "") \
        .option("nameField", "fileName") \
        .mode("overwrite") \
        .save(output_folder)

    return os.path.join(output_folder, filename)


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

    pipeline = load_pipeline()

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
                output_local = process_file(pipeline, per_file_folder, filename, tmp_output_folder)
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
                shutil.rmtree(tmp_output_folder, ignore_errors=True)
                os.makedirs(tmp_output_folder, exist_ok=True)
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
