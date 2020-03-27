# Databricks notebook source
# MAGIC %md # Example of usage Spark OCR
# MAGIC * Load example PDF
# MAGIC * Preview it
# MAGIC * Recognize text

# COMMAND ----------

# MAGIC %md ## Add init script for install fresh version of Tesseract
# MAGIC Note: Need restart cluster after added script if run first time on Databrick accaunt.

# COMMAND ----------

from sparkocr.databricks import create_init_script_for_tesseract
create_init_script_for_tesseract()

# COMMAND ----------

# MAGIC %md ## Check tesseract installation
# MAGIC Need tesseract 4.1.1

# COMMAND ----------

# MAGIC %sh
# MAGIC tesseract -v

# COMMAND ----------

# MAGIC %md ## Import OCR transformers and utils

# COMMAND ----------

from sparkocr.transformers import *
from sparkocr.databricks import display_images
from pyspark.ml import PipelineModel

# COMMAND ----------

# MAGIC %md ## Define OCR transformers and pipeline
# MAGIC * Transforrm binary data to Image schema using [BinaryToImage](https://nlp.johnsnowlabs.com/docs/en/ocr#binarytoimage). More details about Image Schema [here]( https://nlp.johnsnowlabs.com/docs/en/ocr#image-schema).
# MAGIC * Recognize text using [TesseractOcr](https://nlp.johnsnowlabs.com/docs/en/ocr#tesseractocr) transformer.

# COMMAND ----------

def pipeline():
    
    # Transforrm PDF document to struct image format
    pdf_to_image = PdfToImage()
    pdf_to_image.setInputCol("content")
    pdf_to_image.setOutputCol("image")
    pdf_to_image.setResolution(200)

    # Run tesseract OCR
    ocr = TesseractOcr()
    ocr.setInputCol("image")
    ocr.setOutputCol("text")
    ocr.setConfidenceThreshold(65)
    
    pipeline = PipelineModel(stages=[
        pdf_to_image,
        ocr
    ])
    
    return pipeline

# COMMAND ----------

# MAGIC %md ## Copy example files from OCR resources to DBFS

# COMMAND ----------

import pkg_resources
import shutil, os
ocr_examples = "/dbfs/FileStore/examples"
resources = pkg_resources.resource_filename('sparkocr', 'resources')
if not os.path.exists(ocr_examples):
  shutil.copytree(resources, ocr_examples)

# COMMAND ----------

# MAGIC %fs ls /FileStore/examples/ocr/pdfs

# COMMAND ----------

# MAGIC %md ## Read PDF document as binary file from DBFS

# COMMAND ----------

pdf_example = '/FileStore/examples/ocr/pdfs/test_document.pdf'
pdf_example_df = spark.read.format("binaryFile").load(pdf_example).cache()
display(pdf_example_df)

# COMMAND ----------

# MAGIC %md ## Preview PDF using _display_images_ function

# COMMAND ----------

display_images(PdfToImage().setOutputCol("image").transform(pdf_example_df), limit=3)

# COMMAND ----------

# MAGIC %md ## Run OCR pipelines

# COMMAND ----------

result = pipeline().transform(pdf_example_df).cache()

# COMMAND ----------

# MAGIC %md ## Display results

# COMMAND ----------

display(result.select("pagenum", "text", "confidence"))

# COMMAND ----------

# MAGIC %md ## Clear cache

# COMMAND ----------

result.unpersist()
pdf_example_df.unpersist()

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/databricks/init/")
dbutils.fs.put("/databricks/init/tesseract-install.sh","""
#!/bin/bash
sudo add-apt-repository ppa:alex-p/tesseract-ocr
sudo apt-get update
sudo apt-get install -y tesseract-ocr
tesseract -v""", True)
