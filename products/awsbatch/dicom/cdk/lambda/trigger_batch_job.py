import os
import re
import logging

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

batch = boto3.client("batch")

JOB_QUEUE_ARN = os.environ["JOB_QUEUE_ARN"]
JOB_DEFINITION_ARN = os.environ["JOB_DEFINITION_ARN"]
READY_MARKER = "_READY"


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

    input_uri = f"s3://{bucket}/{folder_prefix}/"
    output_uri = f"s3://{bucket}/{output_prefix}"

    job_name = re.sub(r"[^A-Za-z0-9_-]", "-", f"deid-{folder_name}")[:128]

    logger.info("Submitting Batch job %s: %s -> %s", job_name, input_uri, output_uri)

    response = batch.submit_job(
        jobName=job_name,
        jobQueue=JOB_QUEUE_ARN,
        jobDefinition=JOB_DEFINITION_ARN,
        containerOverrides={
            "environment": [
                {"name": "INPUT_S3_URI", "value": input_uri},
                {"name": "OUTPUT_S3_URI", "value": output_uri},
            ]
        },
    )

    return {"jobId": response["jobId"], "jobName": job_name}
