# How to install OCR in 1 line of code in Goole Colab and AWS Sagemaker 

## 1. Upload secrets  
Upload your `spark_ocr.json` and `spark_nlp_for_healthcare.json` to the default Google Colab or Sagemaker directory.

## 2. Set environment variables 
After uploading the json files run the following script in a cell after the upload to set environment variables.
You need to re-run after every notebook restart.
```python
import json
import os

# Upload your spark_nlp_for_healthcare.json  to the default directory and then run this cell to set env variables 
with open('spark_nlp_for_healthcare.json', 'r') as f:
    for k, v in json.load(f).items():
        %set_env $k=$v

# Upload your spark_ocr.json  to the default directory and then run this cell to set env variables 
with open('spark_ocr.json', 'r') as f:
    for k, v in json.load(f).items():
        %set_env $k=$v
        if k == 'SPARK_OCR_LICENSE' :
            k = 'JSL_OCR_LICENSE'
            %set_env $k=$v
        if k == 'JSL_OCR_SECRET' :
            k = 'SPARK_OCR_SECRET'
            %set_env $k=$v
```


## 3. Run the 1-line install script
For `AWS Sagemaker`
``sh
!wget https://raw.githubusercontent.com/JohnSnowLabs/spark-ocr-workshop/master/sh/jsl_sagemaker_setup_with_OCR.sh -O - | bash

``
For `Google Colab`
``sh
!wget https://raw.githubusercontent.com/JohnSnowLabs/spark-ocr-workshop/master/sh/jsl_sagemaker_setup_with_OCR.sh -O - | bash
``

## 4. Restart Notebook and run Code from (2.) again
The Python kernel must be restarted for the new packages to become importable

## 5. Start a Spark Session with OCR 
```
import os 
import sparkocr
from sparkocr import start
print(f"OCR VERSION: {sparkocr.version()}")
spark = start(secret=os.environ['SPARK_OCR_SECRET'], nlp_secret=os.environ['SECRET'], nlp_version=os.environ['PUBLIC_VERSION'], nlp_internal=os.environ['JSL_VERSION'])
```


