#!/usr/bin/env python3
import aws_cdk as cdk

from deid_pipeline.deid_stack import DeidPipelineStack

app = cdk.App()
DeidPipelineStack(app, "DeidPipelineStack")
app.synth()
