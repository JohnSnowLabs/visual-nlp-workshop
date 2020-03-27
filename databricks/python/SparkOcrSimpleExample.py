# Databricks notebook source
# MAGIC %md # Example of usage Spark OCR
# MAGIC * Load images from S3
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
    
    # Transforrm binary data to struct image format
    binary_to_image = BinaryToImage()
    binary_to_image.setInputCol("content")
    binary_to_image.setOutputCol("image")

    # Run tesseract OCR
    ocr = TesseractOcr()
    ocr.setInputCol("image")
    ocr.setOutputCol("text")
    ocr.setConfidenceThreshold(65)
    
    pipeline = PipelineModel(stages=[
        binary_to_image,
        ocr
    ])
    
    return pipeline

# COMMAND ----------

# MAGIC %md ## Download images from public S3 bucket to DBFS

# COMMAND ----------

# MAGIC %sh
# MAGIC OCR_DIR=/dbfs/tmp/ocr
# MAGIC if [ ! -d "$OCR_DIR" ]; then
# MAGIC     mkdir $OCR_DIR
# MAGIC     cd $OCR_DIR
# MAGIC     wget https://s3.amazonaws.com/auxdata.johnsnowlabs.com/public/ocr/datasets/images.zip
# MAGIC     unzip images.zip
# MAGIC fi

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/tmp/ocr/images/"))

# COMMAND ----------

# MAGIC %md ## Read images as binary files
# MAGIC from DBFS

# COMMAND ----------

images_path = "/tmp/ocr/images/*.tif"
images_example_df = spark.read.format("binaryFile").load(images_path).cache()
display(images_example_df)

# COMMAND ----------

# MAGIC %md ## Read data from s3 directly using credentials

# COMMAND ----------

# ACCESS_KEY = ""
# SECRET_KEY = ""
# sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", ACCESS_KEY)
# sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", SECRET_KEY)
# imagesPath = "s3a://dev.johnsnowlabs.com/ocr/datasets/news.2B/0/*.tif"
# imagesExampleDf = spark.read.format("binaryFile").load(imagesPath).cache()
# display(imagesExampleDf)

# COMMAND ----------

# MAGIC %md ## Display count of images

# COMMAND ----------

images_example_df.count()

# COMMAND ----------

# MAGIC %md ## Preview images using _display_images_ function

# COMMAND ----------

display_images(BinaryToImage().transform(images_example_df), limit=3)

# COMMAND ----------

# MAGIC %md ## Run OCR pipelines

# COMMAND ----------

result = pipeline().transform(images_example_df).cache()

# COMMAND ----------

# MAGIC %md ## Display results

# COMMAND ----------

display(result.select("text", "confidence"))

# COMMAND ----------

# MAGIC %md ## Clear cache

# COMMAND ----------

result.unpersist()
