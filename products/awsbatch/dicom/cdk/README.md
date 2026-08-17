# De-id pipeline infrastructure

S3 → EventBridge → Lambda → AWS Batch (EC2, `c7a.4xlarge`) → container (`../docker`).

Flow: files land under `s3://<bucket>/<folder>/`. Nothing happens until a
`_READY` object is created under that same prefix. That triggers an
EventBridge rule (S3 → EventBridge notifications, filtered on the `_READY`
key suffix) which invokes a Lambda that submits a Batch job with
`INPUT_S3_URI=s3://<bucket>/<folder>/` and
`OUTPUT_S3_URI=s3://<bucket>/<folder>_output/`. On failure the container
writes `_FAILURE_{filename}` (containing the error) to the output prefix.

## Prerequisites

- AWS CLI configured with credentials for the target account.
- Node.js (for the `aws-cdk` CLI) and Python 3.10+.
- Docker, for building the container image.
- `npx aws-cdk` works without a global install; use that if you don't want
  `aws-cdk` installed globally.

## One-time environment setup

```bash
cd cdk
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Only needed once per account/region:
npx aws-cdk bootstrap aws://<ACCOUNT_ID>/<REGION>
```

## Deploy

There are two ways to manage the ECR repository, controlled by the
`ecr_repository_name` context value:

- **Stack-owned repo (default, no context passed)**: `cdk deploy` creates its
  own ECR repository. Simple for a single account working alone, but the
  repo (and whatever image is in it) is destroyed with `cdk destroy`, and
  the repo doesn't exist until *after* the first deploy — so you can't push
  an image before deploying at least once.
- **Pre-existing repo (`-c ecr_repository_name=<name>`)**: you create the
  ECR repo yourself first, push the image to it, then deploy the stack
  pointing at it. The repo's lifecycle is independent of the stack's —
  `cdk destroy` never touches it, and pushing a new image never requires a
  redeploy. This is the flow used for handing this off to another account —
  see `../README.md` for the full ordered runbook.

`license_file` is always required (either flavor) — the stack reads that
local file at synth time and writes its `SPARK_OCR_LICENSE` field directly
into the Secrets Manager secret it creates, so there's no separate step to
upload the secret afterward. `deid_stack.py` itself is generic and safe to
commit; the license file is private, per-account, and never committed.

`bucket_name` is optional — omit it to let CloudFormation auto-generate a
name (only knowable afterward, from the stack outputs), or pass it to pick
the name yourself up front. Bucket names are globally unique across all of
AWS, so it must not collide with any bucket in any account.

```bash
# stack-owned repo:
npx aws-cdk deploy -c license_file=path/to/your-license.json -c bucket_name=<globally-unique-bucket-name>

# pre-existing repo:
npx aws-cdk deploy -c ecr_repository_name=<name> -c license_file=path/to/your-license.json -c bucket_name=<globally-unique-bucket-name>
```

Either way this creates the S3 bucket, VPC, Batch compute environment/queue/
job definition, Lambda, EventBridge rule, and the license secret (already
populated). Note the stack outputs — you'll need `ContainerRepositoryUri`.

To inspect the generated CloudFormation without deploying (still needs
`-c license_file=` — the value gets embedded in the template, so treat the
synth output as sensitive):

```bash
npx aws-cdk synth -c license_file=path/to/your-license.json > deid-stack.yaml
```

### Build and push the container image

```bash
REPO_URI=<ContainerRepositoryUri, either from stack output or from
          `aws ecr describe-repositories` if you created it yourself>
aws ecr get-login-password --region <REGION> | docker login --username AWS --password-stdin "${REPO_URI%%/*}"

docker build --secret id=license,src=path/to/your-license.json -t dicom-deid-container ../docker
docker tag dicom-deid-container:latest "${REPO_URI}:latest"
docker push "${REPO_URI}:latest"
```

Batch pulls `:latest` from ECR on every job submission, so pushing a new
image does not require a CDK redeploy. Use `-c image_tag=<tag>` on
`cdk deploy`/`cdk synth` if you want the job definition pinned to a specific
tag instead of `latest`.

## Test end-to-end

```bash
BUCKET=<BucketName from stack output>
aws s3 cp file1.dcm s3://$BUCKET/testfolder/
aws s3 cp file2.dcm s3://$BUCKET/testfolder/
# Nothing runs yet. This triggers the job:
aws s3api put-object --bucket $BUCKET --key testfolder/_READY --body /dev/null

# Watch the job:
aws batch list-jobs --job-queue <JobQueueArn from stack output>
aws logs tail /aws/batch/... --follow   # or the log group from CloudWatch console

# Results:
aws s3 ls s3://$BUCKET/testfolder_output/
```

## Teardown

```bash
npx aws-cdk destroy
```

Notes:

- The S3 bucket and ECR repository are configured to auto-empty and delete
  with the stack (`RemovalPolicy.DESTROY` + `autoDeleteObjects`/
  `emptyOnDelete`). If you don't want processed data destroyed on teardown,
  change those to `RemovalPolicy.RETAIN` in `deid_pipeline/deid_stack.py`
  before deploying.
- The license secret is also set to `DESTROY`, but Secrets Manager still
  enforces a recovery window (default 30 days) unless force-deleted:
  ```bash
  aws secretsmanager delete-secret --secret-id <arn> --force-delete-without-recovery
  ```
  (`cdk destroy` already issues the scheduled deletion; this is only needed
  if you want it gone immediately.)
- If `cdk destroy` fails because the ECR repository still has images, delete
  them first: `aws ecr batch-delete-image --repository-name <name> --image-ids imageTag=latest`.
- `INSTANCE_TYPES`/`JOB_VCPUS`/`JOB_MEMORY_MIB` in `deid_pipeline/deid_stack.py`
  are inherited from the `svs/` stack (benchmarked for gigapixel whole-slide
  tiling) and have not been benchmarked for DICOM workloads, which are almost
  certainly much lighter. Safe to size down once measured.

## Container code

`docker/app.py` wires up the Batch I/O contract (env vars, S3 download/
upload, `_FAILURE` marker) and starts the session via `nlp.start(visual=True)`,
which reads `SPARK_OCR_LICENSE` directly from the environment. `AWS_ACCESS_
KEY_ID`/`AWS_SECRET_ACCESS_KEY` from the license file are only used at image
build time by `docker/installer.py` (a Docker build secret) — not needed at
runtime, so they're never passed to the container. `load_pipeline()` loads
the pretrained `dicom_deid_full_anonymization` pipeline baked into the image
at build time (`/opt/ml/dicom_pipeline`, produced by `installer.py`), and
`process_file()` runs it per file (OCR text detection, blanket redaction of
every detected text region in the image, DICOM metadata de-id) and writes
the result via Spark's `binaryFormat` DICOM writer.
