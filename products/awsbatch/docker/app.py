import os
import logging
import sys
import traceback
import boto3
import tempfile
import argparse
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
    """Load the Spark OCR de-id pipeline.

    Either load a model baked into the image at build time (see
    docker/installer.py) from a local path, or download one at runtime
    (e.g. via johnsnowlabs' pretrained pipeline API) if it isn't present.
    """
    bin_to_image = BinaryToImage() \
    .setInputCol("content") \
    .setOutputCol("image_raw") \
    .setImageType(ImageType.TYPE_BYTE_GRAY) \
    .setKeepInput(False)

    text_detector = ImageTextDetector.load("/opt/ml/image_text_detector_mem_opt") \
    .setInputCol("image_raw") \
    .setOutputCol("text_regions") \
    .setScoreThreshold(0.7) \
    .setLinkThreshold(0.5) \
    .setWithRefiner(True) \
    .setTextThreshold(0.4) \
    .setSizeThreshold(-1) \
    .setUseGPU(False) \
    .setWidth(640)
    
    pipeline = PipelineModel.load("/opt/ml/model")
    pipeline.stages =  [bin_to_image, text_detector] + pipeline.stages[2:]
    return pipeline

def process_file(deid_pipeline, input_file, output_folder):
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
    result_deid = deid_pipeline.transform(image_df)
    deid_info = result_deid.select("path", "coordinates").distinct()
    
    redact_phi_in_tiles("deid_svs_copy", deid_info, tiles_output_tmp, output_svs_path=output_folder, create_new_svs_file = True)
    
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


def write_failure_marker(s3, output_s3, error_message):
    out_bucket, out_prefix = parse_s3_uri(output_s3)
    key = os.path.join(out_prefix, "_FAILURE") if out_prefix else "_FAILURE"
    s3.put_object(Bucket=out_bucket, Key=key, Body=error_message.encode("utf-8"))
    logger.info("Wrote failure marker to s3://%s/%s", out_bucket, key)


# ---- main ----
def process_folder(s3, input_s3, output_s3):
    in_bucket, in_prefix = parse_s3_uri(input_s3)
    out_bucket, out_prefix = parse_s3_uri(output_s3)

    pipeline = load_pipeline()

    with tempfile.TemporaryDirectory() as tmpdir, tempfile.TemporaryDirectory() as output_folder:
        keys = list(list_s3_files(s3, in_bucket, in_prefix))
        if not keys:
            raise ValueError(f"No input files found under s3://{in_bucket}/{in_prefix}")

        for key in keys:
            filename = os.path.basename(key)
            local_path = os.path.join(tmpdir, filename)

            logger.info("Downloading %s...", key)
            s3.download_file(in_bucket, key, local_path)

            print("Processing {filename}")
            output_local = process_file(img_p, nlp_p, tmpdir, output_folder)

            out_key = os.path.join(out_prefix, filename)

            print(f"Uploading to {out_key}..." )
            s3.upload_file(output_local + '/' + filename, out_bucket, out_key)

            os.remove(local_path)
            os.remove(output_local)

    logger.info("Done!")


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
        process_folder(s3_client, input_s3, output_s3)
    except Exception:
        error_message = traceback.format_exc()
        logger.error(error_message)
        try:
            write_failure_marker(s3_client, output_s3, error_message)
        except Exception:
            logger.exception("Failed to write _FAILURE marker to %s", output_s3)
        sys.exit(1)
