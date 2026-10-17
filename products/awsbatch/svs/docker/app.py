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
import pwd
import socket
import threading
from urllib.parse import urlparse
import tifffile
from johnsnowlabs import nlp
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col
from sparkocr.enums import *
from sparkocr.transformers import *

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
    # nlp.start() reads the license from SPARK_OCR_LICENSE (the Batch job secret) or, without it,
    # from the copy installer.py left in the image. nlp.start() also exports the license's AWS keys,
    # which are only needed at build time: restore the environment so boto3 uses the Batch job's
    # IAM role for S3.
    aws_env = {k: os.environ.get(k) for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")}
    session = nlp.start(visual=True, hardware_target=HARDWARE_TARGET)
    for key, value in aws_env.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    return session

spark = None

def load_pipeline():
    """
    Load the pipeline for DEID_MODE: the text detector alone for blanket, the de-identification pipeline for pipeline.
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

def count_tiles(svs_path):
    """Tiles of all pyramid levels."""
    with tifffile.TiffFile(svs_path) as tif:
        # pyramid levels: the tiled pages with the base image's aspect ratio
        tiled = [p for p in tif.pages if p.is_tiled and len(p.shape) >= 2]
        base = max(tiled, key=lambda p: p.shape[0] * p.shape[1])
        ratio = base.shape[1] / base.shape[0]
        levels = [p for p in tiled if abs(p.shape[1] / p.shape[0] - ratio) < 0.03 * ratio]
        return sum(-(-p.shape[1] // p.tilewidth) * -(-p.shape[0] // p.tilelength) for p in levels)


def process_file(pipeline, input_folder, filename, output_folder):
    """Run the de-id pipeline on the local file `filename` in input_folder and return the local
    path to its output file inside output_folder, and its tile counts."""
    from sparkocr.utils.svs.phi_cleaning import remove_phi
    from sparkocr.utils.svs.deidentify import svs_to_images, get_overlapping_tiles, redact_phi_in_svs

    cleaned_header_tmp = tempfile.mkdtemp(dir=output_folder, prefix="cleaned_header_")
    remove_phi(input_folder, cleaned_header_tmp, verbose=True, rename=False)
    fully_qualified_filename = os.path.join(cleaned_header_tmp, filename)

    images_tmp = tempfile.mkdtemp(dir=output_folder, prefix="images_")
    images = svs_to_images(fully_qualified_filename, images_tmp)
    to_image = BinaryToImage().setInputCol("content").setOutputCol("image_raw").setImageType(ImageType.TYPE_BYTE_GRAY)
    result = pipeline.transform(to_image.transform(spark.read.format("binaryFile").load(images).repartition(4))).cache()
    try:
        # all the text the pipeline found, for the counts: pipeline mode redacts only the PHI in it
        text = result if DEID_MODE == "blanket" else result.select("path", col("text_regions").alias("coordinates"))
        tiles_with_text = len(get_overlapping_tiles(fully_qualified_filename, text)[filename])
        tiles_redacted = len(get_overlapping_tiles(fully_qualified_filename, result)[filename])

        create_new_svs_file = os.environ.get("CREATE_NEW_SVS_FILE", "false").lower() == "true"
        output_svs_path = os.path.join(output_folder, filename) if create_new_svs_file else None
        redact_phi_in_svs(fully_qualified_filename, result, output_svs_path, create_new_svs_file)
    finally:
        result.unpersist()

    tiles = {
        "tiles": count_tiles(fully_qualified_filename),
        "tiles_with_text": tiles_with_text,
        "tiles_redacted": tiles_redacted,
    }
    if create_new_svs_file:
        fully_qualified_filename = output_svs_path

    return fully_qualified_filename, tiles


# ---- storage helpers: s3://bucket/prefix/ or a local folder (e.g. a mounted network drive) ----
_s3 = None


def s3_client():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def is_s3(uri):
    return uri.startswith("s3://")


def parse_s3_uri(uri):
    parsed = urlparse(uri)
    return parsed.netloc, parsed.path.lstrip("/")


def join(folder, name):
    return folder.rstrip("/") + "/" + name if is_s3(folder) else os.path.join(folder, name)


def list_files(folder, exclude=None):
    """(path, size, relative path) of every file under `folder` and its subfolders, skipping markers
    and other `_` files, and the folder `exclude` (the output, when it is inside the input)."""
    if is_s3(folder):
        bucket, prefix = parse_s3_uri(folder)
        prefix = prefix.rstrip("/") + "/" if prefix else ""
        skip = parse_s3_uri(exclude)[1].rstrip("/") + "/" if exclude and is_s3(exclude) else None
        paginator = s3_client().get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith("/") and not os.path.basename(key).startswith("_") \
                        and not (skip and key.startswith(skip)):
                    yield f"s3://{bucket}/{key}", obj["Size"], key[len(prefix):]
        return
    skip = os.path.realpath(exclude) if exclude else None
    for root, dirs, names in os.walk(folder):
        dirs[:] = sorted(d for d in dirs if not d.startswith("_")
                         and os.path.realpath(os.path.join(root, d)) != skip)
        for name in sorted(names):
            path = os.path.join(root, name)
            if not name.startswith("_") and not name.endswith(".part") and os.path.isfile(path):
                yield path, os.path.getsize(path), os.path.relpath(path, folder).replace(os.sep, "/")


def marker_name(rel, prefix):
    """The marker of the file at `rel`, in the same subfolder: a/b/abc.svs -> a/b/_SUCCESS_abc.svs."""
    folder, name = os.path.split(rel)
    return f"{folder}/{prefix}{name}" if folder else f"{prefix}{name}"


def download(path, local_path):
    if is_s3(path):
        s3_client().download_file(*parse_s3_uri(path), local_path)
    else:
        shutil.copyfile(path, local_path)


def upload(local_path, path):
    if is_s3(path):
        s3_client().upload_file(local_path, *parse_s3_uri(path))
    else:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # copy then rename, so a file in the output is always complete
        shutil.copyfile(local_path, path + ".part")
        os.replace(path + ".part", path)


def write_marker(output, name, body=""):
    path = join(output, name)
    if is_s3(path):
        bucket, key = parse_s3_uri(path)
        s3_client().put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
    else:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(body)
    logger.info("Wrote marker %s", path)


def has_marker(output, name):
    path = join(output, name)
    if not is_s3(path):
        return os.path.exists(path)
    bucket, key = parse_s3_uri(path)
    try:
        s3_client().head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def remove_marker(output, name):
    path = join(output, name)
    if is_s3(path):
        bucket, key = parse_s3_uri(path)
        s3_client().delete_object(Bucket=bucket, Key=key)
    elif os.path.exists(path):
        os.remove(path)


def write_failure_marker(output, error_message, rel=None):
    write_marker(output, marker_name(rel, "_FAILURE_") if rel else "_FAILURE", error_message)


METRICS = ["file_name", "file_size_bytes", "tiles", "tiles_with_text", "tiles_redacted",
           "processing_seconds", "status"]
RUN_ID = os.environ.get("AWS_BATCH_JOB_ID", f"local_{time.strftime('%Y%m%dT%H%M%S')}_{socket.gethostname()}")


def write_metrics(output, rows):
    """One csv per job attempt (or per local run) in the output folder, rewritten after every file."""
    name = f"metrics_{RUN_ID.replace(':', '_')}_{os.environ.get('AWS_BATCH_JOB_ATTEMPT', '1')}.csv"
    with tempfile.NamedTemporaryFile("w", newline="", suffix=".csv") as f:
        writer = csv.DictWriter(f, fieldnames=METRICS)
        writer.writeheader()
        writer.writerows(rows)
        f.flush()
        upload(f.name, join(output, name))


# ---- claims: several containers can share a local output folder, each file is processed by one of them ----
# a claim not refreshed for this long is from a task that stopped, and the file is taken again
CLAIM_TIMEOUT = float(os.environ.get("CLAIM_TIMEOUT_MINUTES", "30")) * 60


def claim(output, rel):
    """Claim the file at `rel` for this task: mkdir is atomic, also on a network drive."""
    path = join(output, marker_name(rel, "_CLAIM_"))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        os.mkdir(path)
        return path
    except FileExistsError:
        pass
    try:
        if time.time() - os.path.getmtime(path) < CLAIM_TIMEOUT:
            return None
        # stale: only the task that renames it away takes the file
        os.rename(path, f"{path}.{RUN_ID}.stale")
        shutil.rmtree(f"{path}.{RUN_ID}.stale", ignore_errors=True)
        os.mkdir(path)
        return path
    except OSError:
        return None


class Heartbeat:
    """Keeps a claim fresh while its file is processed."""

    def __init__(self, path):
        self.path, self.stop = path, threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        while not self.stop.wait(CLAIM_TIMEOUT / 6):
            try:
                os.utime(self.path)
            except OSError:
                pass

    def __enter__(self):
        self.thread.start()

    def __exit__(self, *exc):
        self.stop.set()
        shutil.rmtree(self.path, ignore_errors=True)


def input_files(output=None):
    """(path, relative path) of the files to process: this job's manifest (MANIFEST_S3_URI, one per
    array child), or the files under INPUT_URI and its subfolders, largest first."""
    manifest_s3 = os.environ.get("MANIFEST_S3_URI")
    if manifest_s3:
        bucket, prefix = parse_s3_uri(manifest_s3)
        index = os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX", "0")
        body = s3_client().get_object(Bucket=bucket, Key=os.path.join(prefix, f"{index}.json"))["Body"].read()
        manifest = json.loads(body)
        prefix = manifest.get("prefix", "")
        return [(f"s3://{manifest['bucket']}/{key}", key[len(prefix):] if prefix else os.path.basename(key))
                for key in manifest["keys"]]
    files = sorted(list_files(os.environ["INPUT_URI"], exclude=output), key=lambda f: (-f[1], f[0]))
    return [(path, rel) for path, _, rel in files]


# ---- main ----
def process_one(pipeline, path, rel, output, tmp_input_folder, tmp_output_folder):
    """De-identify one file into output at the same relative path `rel` as in the input, with its
    _SUCCESS_ or _FAILURE_ marker; returns its metrics."""
    filename = os.path.basename(path)
    per_file_folder = tempfile.mkdtemp(dir=tmp_input_folder, prefix='tmp_input')
    per_file_output = tempfile.mkdtemp(dir=tmp_output_folder, prefix='tmp_output')
    local_path = os.path.join(per_file_folder, filename)
    row = {"file_name": rel, "status": "failed"}
    start = time.time()

    try:
        logger.info("Downloading %s...", path)
        download(path, local_path)
        row["file_size_bytes"] = os.path.getsize(local_path)

        logger.info("Processing %s...", filename)
        output_local, tiles = process_file(pipeline, per_file_folder, filename, per_file_output)
        row.update(tiles)
        out_path = join(output, rel)

        logger.info("Uploading to %s...", out_path)
        upload(output_local, out_path)
        write_marker(output, marker_name(rel, "_SUCCESS_"))
        remove_marker(output, marker_name(rel, "_FAILURE_"))
        row["status"] = "success"
    except Exception:
        error_message = traceback.format_exc()
        logger.error("Failed to process %s:\n%s", rel, error_message)
        try:
            write_failure_marker(output, error_message, rel=rel)
        except Exception:
            logger.exception("Failed to write failure marker for %s", rel)
    finally:
        # clean up this file only, keep parent dirs for the next one
        shutil.rmtree(per_file_output, ignore_errors=True)
        shutil.rmtree(per_file_folder, ignore_errors=True)

    row["processing_seconds"] = round(time.time() - start, 1)
    logger.info("Metrics %s: size=%s bytes, tiles=%s, tiles with text=%s, tiles redacted=%s, time=%ss, %s",
                rel, row.get("file_size_bytes"), row.get("tiles"), row.get("tiles_with_text"),
                row.get("tiles_redacted"), row["processing_seconds"], row["status"])
    return row


def process_files(files, output):
    """De-identify `files` ((path, relative path) pairs) into output, keeping their subfolders, and
    skipping those with a _SUCCESS_ marker. With a local output
    each file is claimed first, so any number of containers can run on the same folder; the folder is
    listed again until there is nothing left to claim. Returns the metrics of the files processed."""
    if not files:
        raise ValueError("No input files found")
    shared = not is_s3(output) and not os.environ.get("MANIFEST_S3_URI")
    retry_failed = os.environ.get("RETRY_FAILED", "false").lower() == "true"
    tried = set()

    pipeline = load_pipeline()
    metrics = []

    with tempfile.TemporaryDirectory() as tmp_input_folder, tempfile.TemporaryDirectory() as tmp_output_folder:
        while True:
            processed = 0
            for path, rel in files:
                if has_marker(output, marker_name(rel, "_SUCCESS_")):
                    logger.info("Skipping %s, already de-identified", rel)
                    continue
                if not shared:
                    metrics.append(process_one(pipeline, path, rel, output, tmp_input_folder, tmp_output_folder))
                else:
                    if rel in tried:
                        continue
                    # a failed file is retried only when it was replaced, or with --retry-failed
                    failure = join(output, marker_name(rel, "_FAILURE_"))
                    if os.path.exists(failure) and not retry_failed \
                            and (is_s3(path) or os.path.getmtime(path) <= os.path.getmtime(failure)):
                        logger.info("Skipping %s, it failed before", rel)
                        continue
                    claimed = claim(output, rel)
                    if not claimed:
                        logger.info("Skipping %s, claimed by another task", rel)
                        continue
                    with Heartbeat(claimed):
                        # it may have been finished between the check and the claim
                        if has_marker(output, marker_name(rel, "_SUCCESS_")):
                            continue
                        tried.add(rel)
                        metrics.append(process_one(pipeline, path, rel, output, tmp_input_folder, tmp_output_folder))
                processed += 1
                try:
                    write_metrics(output, metrics)
                except Exception:
                    logger.exception("Failed to write the metrics csv")
            if not shared or not processed:
                break
            files = input_files(output)

    failed_files = [row["file_name"] for row in metrics if row["status"] != "success"]
    if failed_files:
        logger.error("%d of %d file(s) failed: %s", len(failed_files), len(metrics), failed_files)
    logger.info("Done!")
    return metrics


def get_config():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=False, help="s3://bucket/prefix/ or a local folder")
    parser.add_argument("--output", required=False, help="s3://bucket/prefix/ or a local folder")
    parser.add_argument("--retry-failed", action="store_true",
                        help="local folders: also retry the files with a _FAILURE_ marker")
    args, _ = parser.parse_known_args()

    if args.retry_failed:
        os.environ["RETRY_FAILED"] = "true"
    if args.input:
        os.environ["INPUT_URI"] = args.input
    elif os.environ.get("INPUT_S3_URI"):
        os.environ["INPUT_URI"] = os.environ["INPUT_S3_URI"]
    output = args.output or os.environ.get("OUTPUT_URI") or os.environ.get("OUTPUT_S3_URI")

    if not (os.environ.get("INPUT_URI") or os.environ.get("MANIFEST_S3_URI")) or not output:
        raise ValueError(
            "Input/output locations must be provided via --input/--output "
            "or the INPUT_URI (or MANIFEST_S3_URI)/OUTPUT_URI environment variables."
        )
    return output


def ensure_user():
    """Any uid can run the container (e.g. --user to write to a root_squash NFS share), but Spark needs it in /etc/passwd."""
    try:
        pwd.getpwuid(os.getuid())
    except KeyError:
        with open("/etc/passwd", "a") as f:
            f.write(f"deid:x:{os.getuid()}:{os.getgid()}::/tmp:/bin/sh\n")


if __name__ == "__main__":
    output = get_config()
    ensure_user()
    if not is_s3(output):
        os.makedirs(output, exist_ok=True)
    spark = start_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        metrics = process_files(input_files(output), output)
    except Exception:
        error_message = traceback.format_exc()
        logger.error(error_message)
        try:
            write_failure_marker(output, error_message)
        except Exception:
            logger.exception("Failed to write _FAILURE marker to %s", output)
        sys.exit(1)

    # failed files keep their _FAILURE_ marker; the job fails only if every file it processed failed
    if metrics and all(row["status"] != "success" for row in metrics):
        sys.exit(1)
