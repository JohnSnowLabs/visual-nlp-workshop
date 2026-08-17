# De-id pipeline infrastructure

S3 → EventBridge → Lambda → AWS Batch (EC2, `c7a.4xlarge`) → container (`../docker`).

Flow: files land under `s3://<bucket>/<folder>/`. Nothing happens until a
`_READY` object is created under that same prefix. That triggers an
EventBridge rule (S3 → EventBridge notifications, filtered on the `_READY`
key suffix) which invokes a Lambda that submits a Batch job with
`INPUT_S3_URI=s3://<bucket>/<folder>/` and
`OUTPUT_S3_URI=s3://<bucket>/<folder>_output/`. On failure the container
writes `_FAILURE` (containing the error) to the output prefix.

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

docker build --secret id=license,src=path/to/your-license.json -t deid-container ../docker
docker tag deid-container:latest "${REPO_URI}:latest"
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

## Container code

`docker/app.py` wires up the Batch I/O contract (env vars, S3 download/
upload, `_FAILURE` marker) and starts the session via `nlp.start(visual=True)`,
which reads `SPARK_OCR_LICENSE` directly from the environment. `AWS_ACCESS_
KEY_ID`/`AWS_SECRET_ACCESS_KEY` from the license file are only used at image
build time by `docker/installer.py` (a Docker build secret) — not needed at
runtime, so they're never passed to the container. `load_pipeline()` loads
the de-id pipeline baked into the image at build time (`/opt/ml/model` and
`/opt/ml/image_text_detector_mem_opt`, produced by `installer.py` via
`MODEL_TO_LOAD` in the `Dockerfile`), and `process_file()` runs it per file
(header cleanup, tiling, OCR + de-id, redaction). Redaction writes tiles
back in place by default; set `CREATE_NEW_SVS_FILE=true` to have it write a
new de-identified `.svs` file instead (slower, but leaves the original
untouched).


## Permissions
Provide these customer defined IAM policies and attach them to your user,
monitor-lambda, replace account_id, with your account id.
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "CloudFormationStacks",
            "Effect": "Allow",
            "Action": [
                "cloudformation:CreateStack",
                "cloudformation:UpdateStack",
                "cloudformation:DeleteStack",
                "cloudformation:DescribeStacks",
                "cloudformation:DescribeStackEvents",
                "cloudformation:DescribeStackResources",
                "cloudformation:GetTemplate",
                "cloudformation:CreateChangeSet",
                "cloudformation:DescribeChangeSet",
                "cloudformation:ExecuteChangeSet",
                "cloudformation:DeleteChangeSet",
                "cloudformation:ListStacks",
                "cloudformation:GetTemplateSummary",
                "cloudformation:ValidateTemplate"
            ],
            "Resource": [
                "arn:aws:cloudformation:us-east-1:account_id:stack/CDKToolkit/*",
                "arn:aws:cloudformation:us-east-1:account_id:stack/DeidPipelineStack/*"
            ]
        },
        {
            "Sid": "BootstrapS3Bucket",
            "Effect": "Allow",
            "Action": [
                "s3:CreateBucket",
                "s3:PutBucketPolicy",
                "s3:PutBucketVersioning",
                "s3:PutBucketPublicAccessBlock",
                "s3:PutEncryptionConfiguration",
                "s3:PutLifecycleConfiguration",
                "s3:GetBucketPolicy",
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::cdk-hnb659fds-assets-account_id-us-east-1",
                "arn:aws:s3:::cdk-hnb659fds-assets-account_id-us-east-1/*"
            ]
        },
        {
            "Sid": "BootstrapEcrRepo",
            "Effect": "Allow",
            "Action": [
                "ecr:CreateRepository",
                "ecr:DescribeRepositories",
                "ecr:SetRepositoryPolicy",
                "ecr:PutLifecyclePolicy",
                "ecr:GetLifecyclePolicy",
                "ecr:DeleteRepository"
            ],
            "Resource": "arn:aws:ecr:us-east-1:account_id:repository/cdk-hnb659fds-container-assets-account_id-us-east-1"
        },
        {
            "Sid": "BootstrapRoles",
            "Effect": "Allow",
            "Action": [
                "iam:CreateRole",
                "iam:DeleteRole",
                "iam:GetRole",
                "iam:AttachRolePolicy",
                "iam:DetachRolePolicy",
                "iam:PutRolePolicy",
                "iam:DeleteRolePolicy",
                "iam:GetRolePolicy",
                "iam:UpdateAssumeRolePolicy",
                "iam:TagRole"
            ],
            "Resource": "arn:aws:iam::account_id:role/cdk-hnb659fds-*-role-*"
        },
        {
            "Sid": "AssumeBootstrapRolesForDeploy",
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Resource": "arn:aws:iam::account_id:role/cdk-hnb659fds-*-role-*"
        },
        {
            "Sid": "BootstrapVersionParameter",
            "Effect": "Allow",
            "Action": [
                "ssm:GetParameter",
                "ssm:PutParameter",
                "ssm:DeleteParameter"
            ],
            "Resource": "arn:aws:ssm:us-east-1:account_id:parameter/cdk-bootstrap/hnb659fds/version"
        },
        {
            "Sid": "TestDataBucketAccess",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::pathology-images-account_id-us-east-1-an",
                "arn:aws:s3:::pathology-images-account_id-us-east-1-an/*"
            ]
        },
        {
            "Sid": "TailDeidPipelineLogs",
            "Effect": "Allow",
            "Action": "logs:FilterLogEvents",
            "Resource": [
                "arn:aws:logs:us-east-1:account_id:log-group:/aws/lambda/DeidPipelineStack-*:*",
                "arn:aws:logs:us-east-1:account_id:log-group:DeidPipelineStack-*:*"
            ]
        }
    ]
}
```
cdk-bootstrap-minimal
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "CloudFormationStacks",
            "Effect": "Allow",
            "Action": [
                "cloudformation:CreateStack",
                "cloudformation:UpdateStack",
                "cloudformation:DeleteStack",
                "cloudformation:DescribeStacks",
                "cloudformation:DescribeStackEvents",
                "cloudformation:DescribeStackResources",
                "cloudformation:GetTemplate",
                "cloudformation:CreateChangeSet",
                "cloudformation:DescribeChangeSet",
                "cloudformation:ExecuteChangeSet",
                "cloudformation:DeleteChangeSet",
                "cloudformation:ListStacks",
                "cloudformation:GetTemplateSummary",
                "cloudformation:ValidateTemplate"
            ],
            "Resource": [
                "arn:aws:cloudformation:us-east-1:account_id:stack/CDKToolkit/*",
                "arn:aws:cloudformation:us-east-1:account_id:stack/DeidPipelineStack/*"
            ]
        },
        {
            "Sid": "BootstrapS3Bucket",
            "Effect": "Allow",
            "Action": [
                "s3:CreateBucket",
                "s3:PutBucketPolicy",
                "s3:PutBucketVersioning",
                "s3:PutBucketPublicAccessBlock",
                "s3:PutEncryptionConfiguration",
                "s3:PutLifecycleConfiguration",
                "s3:GetBucketPolicy",
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::cdk-hnb659fds-assets-account_id-us-east-1",
                "arn:aws:s3:::cdk-hnb659fds-assets-account_id-us-east-1/*"
            ]
        },
        {
            "Sid": "BootstrapEcrRepo",
            "Effect": "Allow",
            "Action": [
                "ecr:CreateRepository",
                "ecr:DescribeRepositories",
                "ecr:SetRepositoryPolicy",
                "ecr:PutLifecyclePolicy",
                "ecr:GetLifecyclePolicy",
                "ecr:DeleteRepository"
            ],
            "Resource": "arn:aws:ecr:us-east-1:account_id:repository/cdk-hnb659fds-container-assets-account_id-us-east-1"
        },
        {
            "Sid": "BootstrapRoles",
            "Effect": "Allow",
            "Action": [
                "iam:CreateRole",
                "iam:DeleteRole",
                "iam:GetRole",
                "iam:AttachRolePolicy",
                "iam:DetachRolePolicy",
                "iam:PutRolePolicy",
                "iam:DeleteRolePolicy",
                "iam:GetRolePolicy",
                "iam:UpdateAssumeRolePolicy",
                "iam:TagRole"
            ],
            "Resource": "arn:aws:iam::account_id:role/cdk-hnb659fds-*-role-*"
        },
        {
            "Sid": "AssumeBootstrapRolesForDeploy",
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Resource": "arn:aws:iam::account_id:role/cdk-hnb659fds-*-role-*"
        },
        {
            "Sid": "BootstrapVersionParameter",
            "Effect": "Allow",
            "Action": [
                "ssm:GetParameter",
                "ssm:PutParameter",
                "ssm:DeleteParameter"
            ],
            "Resource": "arn:aws:ssm:us-east-1:account_id:parameter/cdk-bootstrap/hnb659fds/version"
        }
    ]
}
```
