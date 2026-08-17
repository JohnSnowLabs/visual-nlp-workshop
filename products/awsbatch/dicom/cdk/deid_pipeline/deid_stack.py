import os
import json

import aws_cdk as cdk
from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    aws_s3 as s3,
    aws_ec2 as ec2,
    aws_ecr as ecr,
    aws_iam as iam,
    aws_batch as batch,
    aws_lambda as lambda_,
    aws_events as events,
    aws_events_targets as targets,
    aws_secretsmanager as secretsmanager,
    aws_logs as logs,
    custom_resources as cr,
)
from constructs import Construct


class DeidPipelineStack(Stack):
    # c7a.2xlarge: 8 vCPU / 16 GiB, sized for the pipeline's actual
    # footprint (~11 vCPU / ~7 GiB peak across the Spark/JVM tree). Leave
    # headroom below the instance's full capacity for the OS and ECS agent.
    INSTANCE_TYPES = ["c7a.2xlarge"]
    JOB_VCPUS = 7
    JOB_MEMORY_MIB = 13000
    MAX_VCPUS = 64

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        image_tag = self.node.try_get_context("image_tag") or "latest"
        repository_name = self.node.try_get_context("ecr_repository_name")

        # `cdk destroy` (and `diff`/`synth` run without deploying) still
        # instantiate this stack to synthesize the template, even though
        # they never need the secret's real value -- and there's no clean
        # way for this code to tell which CLI subcommand triggered it. So
        # license_file is optional: real deploys should always pass it via
        # -c license_file=<path>, but a missing one falls back to a
        # placeholder instead of hard-failing commands that don't need it.
        license_file_path = self.node.try_get_context("license_file")
        if license_file_path:
            license_file_path = os.path.abspath(license_file_path)
            if not os.path.isfile(license_file_path):
                raise ValueError(f"License file not found: {license_file_path}")
            with open(license_file_path, encoding="utf-8") as f:
                license_data = json.load(f)
        else:
            license_file_path = "<none provided -- pass -c license_file=<path> to deploy for real>"
            license_data = {"SPARK_OCR_LICENSE": "MISSING-PASS-license_file-CONTEXT-TO-DEPLOY"}

        def _license_field(field):
            if field not in license_data:
                raise ValueError(
                    f"License file is missing required field '{field}': {license_file_path}"
                )
            return license_data[field]

        # ---------------------------------------------------------------
        # S3 bucket: stores input/output data. Two flavors, same idea as the
        # ECR repository below:
        # - -c bucket_name=<name>: the stack creates a new bucket with that
        #   exact name (instead of an auto-generated one), owned by the
        #   stack's lifecycle (destroyed on `cdk destroy`).
        # - -c existing_bucket_name=<name>: the stack imports a bucket that
        #   already exists (e.g. one already used by another workload, like
        #   a SageMaker bucket). CDK never owns an imported bucket's
        #   lifecycle -- `cdk destroy` leaves it untouched, and CDK cannot
        #   attach the AWS::S3::Bucket EventBridge-notification setting to
        #   it directly (that property only applies to a bucket CDK itself
        #   synthesizes) -- so EventBridge notifications are enabled below
        #   via a custom resource calling the S3 API directly instead.
        # ---------------------------------------------------------------
        bucket_name = self.node.try_get_context("bucket_name")
        existing_bucket_name = self.node.try_get_context("existing_bucket_name")
        if existing_bucket_name:
            bucket = s3.Bucket.from_bucket_name(self, "DataBucket", existing_bucket_name)
            cr.AwsCustomResource(
                self,
                "EnableEventBridgeNotifications",
                on_create=cr.AwsSdkCall(
                    service="S3",
                    action="putBucketNotificationConfiguration",
                    parameters={
                        "Bucket": existing_bucket_name,
                        "NotificationConfiguration": {"EventBridgeConfiguration": {}},
                    },
                    physical_resource_id=cr.PhysicalResourceId.of(
                        f"{existing_bucket_name}-eventbridge-notifications"
                    ),
                ),
                # from_sdk_calls() would auto-derive the IAM action as
                # "s3:PutBucketNotificationConfiguration" (matching the SDK
                # method name), but S3 actually enforces the older
                # "s3:PutBucketNotification" action for this call -- so the
                # policy has to be spelled out explicitly here instead.
                policy=cr.AwsCustomResourcePolicy.from_statements(
                    [
                        iam.PolicyStatement(
                            actions=["s3:PutBucketNotification"],
                            resources=[bucket.bucket_arn],
                        )
                    ]
                ),
            )
        else:
            bucket = s3.Bucket(
                self,
                "DataBucket",
                bucket_name=bucket_name,
                event_bridge_enabled=True,
                removal_policy=RemovalPolicy.DESTROY,
                auto_delete_objects=True,
            )

        # ---------------------------------------------------------------
        # ECR repository for the container image. Built and pushed manually
        # (see ../README.md). Pass -c ecr_repository_name=<name> to import a
        # repository you created and pushed to *before* this deploy, so its
        # lifecycle (and the image in it) is independent of this stack's --
        # `cdk destroy` won't delete an imported repo, and pushing a new
        # image doesn't require a redeploy. Without that context value, the
        # stack creates its own repo (destroyed together with the stack).
        # ---------------------------------------------------------------
        if repository_name:
            repository = ecr.Repository.from_repository_name(
                self, "ContainerRepository", repository_name
            )
        else:
            repository = ecr.Repository(
                self,
                "ContainerRepository",
                removal_policy=RemovalPolicy.DESTROY,
                empty_on_delete=True,
            )
        image_uri = f"{repository.repository_uri}:{image_tag}"

        # ---------------------------------------------------------------
        # License secret: just the SPARK_OCR_LICENSE field, the only license
        # value the running container needs (nlp.start() reads it directly
        # from the environment). AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
        # from the license file are only used at image build time, by
        # docker/installer.py via a Docker build secret -- not needed here.
        # Populated directly from the local license_file at synth time --
        # this stack file is shared/committed, the license file is private
        # and per-account, never committed, only ever referenced by path via
        # -c license_file=.
        # ---------------------------------------------------------------
        license_secret = secretsmanager.Secret(
            self,
            "SparkOcrLicenseSecret",
            description=(
                f"SPARK_OCR_LICENSE for the container, populated from "
                f"{os.path.basename(license_file_path)} at deploy time."
            ),
            removal_policy=RemovalPolicy.DESTROY,
            secret_string_value=cdk.SecretValue.unsafe_plain_text(
                _license_field("SPARK_OCR_LICENSE")
            ),
        )

        # ---------------------------------------------------------------
        # Networking for the Batch EC2 compute environment. Public subnets
        # only (no NAT Gateway) to keep cost down; instances get a public IP
        # for ECR/internet access. Swap for private subnets + NAT if that's
        # not acceptable for this workload.
        # ---------------------------------------------------------------
        vpc = ec2.Vpc(
            self,
            "BatchVpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                )
            ],
        )

        batch_security_group = ec2.SecurityGroup(
            self,
            "BatchSecurityGroup",
            vpc=vpc,
            allow_all_outbound=True,
            description="Outbound-only security group for Batch compute environment instances",
        )

        # ---------------------------------------------------------------
        # IAM: EC2 instance role/profile for the compute environment.
        # ---------------------------------------------------------------
        instance_role = iam.Role(
            self,
            "BatchInstanceRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonEC2ContainerServiceforEC2Role"
                )
            ],
        )
        instance_profile = iam.CfnInstanceProfile(
            self,
            "BatchInstanceProfile",
            roles=[instance_role.role_name],
        )

        batch_service_role = iam.Role(
            self,
            "BatchServiceRole",
            assumed_by=iam.ServicePrincipal("batch.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSBatchServiceRole"
                )
            ],
        )

        # ---------------------------------------------------------------
        # IAM: job role (task role, used by the running container for S3
        # access) and execution role (used by ECS/Batch to pull the image
        # and resolve the Secrets Manager secret before the container
        # starts).
        # ---------------------------------------------------------------
        job_role = iam.Role(
            self,
            "BatchJobRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )
        bucket.grant_read_write(job_role)

        execution_role = iam.Role(
            self,
            "BatchExecutionRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy"
                )
            ],
        )
        license_secret.grant_read(execution_role)

        # ---------------------------------------------------------------
        # Batch: managed EC2 compute environment, job queue, job definition.
        # ---------------------------------------------------------------
        compute_environment = batch.CfnComputeEnvironment(
            self,
            "ComputeEnvironment",
            type="MANAGED",
            service_role=batch_service_role.role_arn,
            compute_resources=batch.CfnComputeEnvironment.ComputeResourcesProperty(
                type="EC2",
                allocation_strategy="BEST_FIT_PROGRESSIVE",
                minv_cpus=0,
                maxv_cpus=self.MAX_VCPUS,
                instance_types=self.INSTANCE_TYPES,
                instance_role=instance_profile.attr_arn,
                security_group_ids=[batch_security_group.security_group_id],
                subnets=vpc.select_subnets(
                    subnet_type=ec2.SubnetType.PUBLIC
                ).subnet_ids,
            ),
        )

        job_queue = batch.CfnJobQueue(
            self,
            "JobQueue",
            priority=1,
            compute_environment_order=[
                batch.CfnJobQueue.ComputeEnvironmentOrderProperty(
                    order=1,
                    compute_environment=compute_environment.ref,
                )
            ],
        )

        job_log_group = logs.LogGroup(
            self,
            "JobLogGroup",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        job_definition = batch.CfnJobDefinition(
            self,
            "JobDefinition",
            type="container",
            platform_capabilities=["EC2"],
            container_properties=batch.CfnJobDefinition.ContainerPropertiesProperty(
                image=image_uri,
                job_role_arn=job_role.role_arn,
                execution_role_arn=execution_role.role_arn,
                resource_requirements=[
                    batch.CfnJobDefinition.ResourceRequirementProperty(
                        type="VCPU", value=str(self.JOB_VCPUS)
                    ),
                    batch.CfnJobDefinition.ResourceRequirementProperty(
                        type="MEMORY", value=str(self.JOB_MEMORY_MIB)
                    ),
                ],
                secrets=[
                    batch.CfnJobDefinition.SecretProperty(
                        name="SPARK_OCR_LICENSE",
                        value_from=license_secret.secret_arn,
                    ),
                ],
                log_configuration=batch.CfnJobDefinition.LogConfigurationProperty(
                    log_driver="awslogs",
                    options={
                        "awslogs-group": job_log_group.log_group_name,
                        "awslogs-region": self.region,
                        "awslogs-stream-prefix": "deid",
                    },
                ),
            ),
            retry_strategy=batch.CfnJobDefinition.RetryStrategyProperty(attempts=1),
            timeout=batch.CfnJobDefinition.TimeoutProperty(attempt_duration_seconds=3600),
        )

        # ---------------------------------------------------------------
        # Lambda: consumes the EventBridge "_READY" event and submits the
        # Batch job. INPUT_S3_URI / OUTPUT_S3_URI are computed per-event and
        # passed as container overrides, not baked into the job definition.
        # ---------------------------------------------------------------
        trigger_fn = lambda_.Function(
            self,
            "TriggerBatchJobFunction",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="trigger_batch_job.handler",
            code=lambda_.Code.from_asset(
                os.path.join(os.path.dirname(__file__), "..", "lambda")
            ),
            timeout=Duration.seconds(30),
            environment={
                "JOB_QUEUE_ARN": job_queue.attr_job_queue_arn,
                "JOB_DEFINITION_ARN": job_definition.attr_job_definition_arn,
            },
        )
        trigger_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["batch:SubmitJob"],
                resources=[
                    job_queue.attr_job_queue_arn,
                    job_definition.attr_job_definition_arn,
                ],
            )
        )

        # ---------------------------------------------------------------
        # EventBridge: only fire on keys ending in "_READY".
        # ---------------------------------------------------------------
        rule = events.Rule(
            self,
            "ReadyMarkerRule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [bucket.bucket_name]},
                    "object": {"key": [{"suffix": "_READY"}]},
                },
            ),
        )
        rule.add_target(targets.LambdaFunction(trigger_fn))

        # ---------------------------------------------------------------
        # Outputs
        # ---------------------------------------------------------------
        cdk.CfnOutput(self, "BucketName", value=bucket.bucket_name)
        cdk.CfnOutput(self, "ContainerRepositoryUri", value=repository.repository_uri)
        cdk.CfnOutput(self, "LicenseSecretArn", value=license_secret.secret_arn)
        cdk.CfnOutput(self, "JobQueueArn", value=job_queue.attr_job_queue_arn)
        cdk.CfnOutput(self, "JobDefinitionArn", value=job_definition.attr_job_definition_arn)
        cdk.CfnOutput(self, "TriggerFunctionName", value=trigger_fn.function_name)
