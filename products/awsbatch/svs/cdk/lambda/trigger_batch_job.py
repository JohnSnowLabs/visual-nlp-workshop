import os
import re
import json
import time
import logging

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

batch = boto3.client("batch")
s3 = boto3.client("s3")

JOB_QUEUE_ARN = os.environ["JOB_QUEUE_ARN"]
JOB_DEFINITION_ARN = os.environ["JOB_DEFINITION_ARN"]
MAX_JOBS = int(os.environ.get("MAX_JOBS", "4"))
# at most this much input per job, so each one ends well within the job timeout
MAX_JOB_BYTES = int(float(os.environ.get("MAX_JOB_GB", "10")) * 1e9)
READY_MARKER = "_READY"
SUCCESS_PREFIX = "_SUCCESS_"
MANIFESTS = "_manifests"


def list_objects(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj


def split(files, jobs):
    """Files spread over at least `jobs` lists of about the same total size (at most MAX_JOB_BYTES
    each when the files allow it), largest first."""
    jobs = max(jobs, int(-(-sum(size for _, size in files) // MAX_JOB_BYTES)))
    jobs = min(jobs, len(files))
    shards = [[] for _ in range(jobs)]
    sizes = [0] * jobs
    for key, size in sorted(files, key=lambda f: -f[1]):
        i = sizes.index(min(sizes))
        shards[i].append(key)
        sizes[i] += size
    return [shard for shard in shards if shard]


def handler(event, context):
    detail = event["detail"]
    bucket = detail["bucket"]["name"]
    key = detail["object"]["key"]

    # The EventBridge rule already filters on the "_READY" key suffix; this
    # check is just defense in depth against a misconfigured rule.
    if not key.endswith(READY_MARKER):
        logger.info("Ignoring %s (not a %s marker)", key, READY_MARKER)
        return {"status": "ignored", "bucket": bucket, "key": key}

    folder_prefix = key[: -len(READY_MARKER)].rstrip("/")
    if not folder_prefix:
        raise ValueError(f"_READY marker at the bucket root is not supported: {key}")

    parent, _, folder_name = folder_prefix.rpartition("/")
    output_prefix = f"{parent}/{folder_name}_output/" if parent else f"{folder_name}_output/"

    # files with a _SUCCESS_ marker in the output are skipped: writing _READY again
    # processes only the ones that failed or never ran
    done = {os.path.basename(obj["Key"])[len(SUCCESS_PREFIX):]
            for obj in list_objects(bucket, output_prefix + SUCCESS_PREFIX)}
    files = [(obj["Key"], obj["Size"]) for obj in list_objects(bucket, folder_prefix + "/")
             if not obj["Key"].endswith("/") and not os.path.basename(obj["Key"]).startswith("_")
             and os.path.basename(obj["Key"]) not in done]
    if not files:
        logger.info("Nothing to process under s3://%s/%s/", bucket, folder_prefix)
        return {"status": "nothing to process", "bucket": bucket, "prefix": folder_prefix}

    # one manifest per job; the jobs run as one array job, each child reads its own manifest, and
    # Batch runs as many at once as the compute environment allows
    shards = split(files, min(MAX_JOBS, len(files)))
    run = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    manifest_prefix = f"{output_prefix}{MANIFESTS}/{run}/"
    for i, shard in enumerate(shards):
        s3.put_object(Bucket=bucket, Key=f"{manifest_prefix}{i}.json",
                      Body=json.dumps({"bucket": bucket, "keys": shard}).encode("utf-8"))

    job_name = re.sub(r"[^A-Za-z0-9_-]", "-", f"deid-{folder_name}")[:128]
    logger.info("Submitting %s: %d file(s) in %d job(s), manifests at s3://%s/%s",
                job_name, len(files), len(shards), bucket, manifest_prefix)

    submit = dict(
        jobName=job_name,
        jobQueue=JOB_QUEUE_ARN,
        jobDefinition=JOB_DEFINITION_ARN,
        containerOverrides={
            "environment": [
                {"name": "MANIFEST_S3_URI", "value": f"s3://{bucket}/{manifest_prefix}"},
                {"name": "OUTPUT_S3_URI", "value": f"s3://{bucket}/{output_prefix}"},
            ]
        },
    )
    if len(shards) > 1:    # an array job needs at least 2 children
        submit["arrayProperties"] = {"size": len(shards)}
    response = batch.submit_job(**submit)

    return {"jobId": response["jobId"], "jobName": job_name, "jobs": len(shards), "files": len(files)}
