# How to install OCR in 1 line of code in Goole Colab and AWS Sagemaker 

## 1. Upload secrets  
Upload your `spark_ocr.json` to the default Google Colab or Sagemaker directory.

## 2. Run the 1-line install script
For `AWS Sagemaker`
```sh
!wget https://raw.githubusercontent.com/JohnSnowLabs/spark-ocr-workshop/master/sh_install_scripts/jsl_sagemaker_setup_with_OCR.sh
!bash jsl_sagemaker_setup_with_OCR.sh spark_ocr.json

```

For `Google Colab`
```sh
!wget https://raw.githubusercontent.com/JohnSnowLabs/spark-ocr-workshop/master/sh_install_scripts/jsl_colab_setup_with_OCR.sh
!bash jsl_colab_setup_with_OCR.sh spark_ocr.json
```

## 3. Restart Notebook/Session
The Python kernel must be restarted for the new packages to become importable

## 4. Grab Credentials from License File
```python
import json
import os

license = "/content/spark_ocr.json"

if license and "json" in license:

    with open(license, "r") as creds_in:
        creds = json.loads(creds_in.read())

        for key in creds.keys():
            os.environ[key] = creds[key]
else:
    raise Exception("License JSON File is not specified")
```

## 5. Start a Spark Session with Visual NLP  
```python
from sparkocr import start
import os

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

extra_configurations = {
    "spark.extraListeners": "com.johnsnowlabs.license.LicenseLifeCycleManager"
}

# jar_path is used Internally for development
spark = start(
    secret = os.environ.get("SPARK_OCR_SECRET"),
    nlp_secret = os.environ.get("SECRET"),
    jar_path = None,
    nlp_internal = os.environ.get("JSL_VERSION"),
    extra_conf=extra_configurations
)

spark
```

## 6. Import Spark + John Snow Labs 
```python
import os
import json
import time
import sys
import shutil 
import pkg_resources 
import pandas as pd
from textwrap import dedent

from sparknlp.annotator import *
from sparknlp.base import *

import sparknlp_jsl
from sparknlp_jsl.annotator import *

import sparkocr
from sparkocr.transformers import *
from sparkocr.utils import *
from sparkocr.enums import *
from sparkocr.schemas import *

from pyspark.ml import PipelineModel, Pipeline
import pyspark.sql.functions as F
from pyspark.sql.types import *

print(f"Spark NLP Version: {sparknlp.version()}")
print(f"Healthcare NLP Version: {sparknlp_jsl.version()}")
print(f"Visual NLP Version: {sparkocr.version()}")
```
