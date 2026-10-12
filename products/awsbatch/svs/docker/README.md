### Docker Container
#### Build 
Here we build the image. The license is passed as a build secret for `installer.py` to install
the licensed jars and model; the installer keeps a copy of it in the image, so keep the image in a
private registry:

```bash
docker build --secret id=license,src=spark_nlp_for_healthcare_spark_ocr_license.json -t deid-container .
```

For GPU, add `--build-arg HARDWARE_TARGET=gpu`.

`DEID_MODE=pipeline` (default) redacts only the PHI found by the de-identification pipeline;
`DEID_MODE=blanket` redacts all detected text. Set it at run time with `-e DEID_MODE=blanket`.

#### Run 
`AWS_*` credentials are for boto3's own S3 access, and are optional if the
container already has them via an IAM role, e.g. a Batch job role):

```bash
docker run --rm \
  -e AWS_ACCESS_KEY_ID="***************" \
  -e AWS_SECRET_ACCESS_KEY="******************************" \
  -e AWS_SESSION_TOKEN="**********************************************"\
  -e AWS_DEFAULT_REGION=******* \
  deid-container \
  --input s3://my.bucket/ocr/svs/ --output s3://my.bucket/ocr/svs_output/
```

On-premise, mount the folder (e.g. a network drive) and pass local paths:

```bash
docker run --rm -v /mnt/drive:/data deid-container --input /data/svs/ --output /data/svs_output/
```

Several containers can run on the same folder, on one or more machines: each file is processed by only one of them. A file whose container stopped is taken by another one after `CLAIM_TIMEOUT_MINUTES` (default 30).
Files that failed are retried when they are replaced, or with `--retry-failed`.
If root can't write to the share (e.g. NFS with `root_squash`), add `--user <uid>:<gid>` of its owner.

See `../README.md` and `../cdk/README.md` for the full Batch-based deployment
(the actual target for this container — the S3 `docker run` above is only for
local testing).


