### Docker Container
#### Build 
Here we build the image, a license is needed here, as a build secret, for `installer.py` to
install the licensed jars/model, not baked into an image layer:

```bash
docker build --secret id=license,src=spark_nlp_for_healthcare_spark_ocr_license.json -t deid-container .
```

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

See `../README.md` and `../cdk/README.md` for the full Batch-based deployment
(the actual target for this container — the `docker run` above is only for
local testing).


