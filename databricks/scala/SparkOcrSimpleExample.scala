// Databricks notebook source
// MAGIC %md # Example of usage Spark OCR
// MAGIC * Load images from S3
// MAGIC * Preview it
// MAGIC * Recognize text

// COMMAND ----------

// MAGIC %md ## Import OCR transformers and utils

// COMMAND ----------

import com.johnsnowlabs.ocr.transformers._
import org.apache.spark.ml.Pipeline
import com.johnsnowlabs.ocr.OcrContext.implicits._
implicit val sparkSession = spark
implicit val displayHtmlFunction: (String) => Unit = displayHTML

// COMMAND ----------

// MAGIC %md ## Define OCR transformers and pipeline
// MAGIC * Transforrm binary data to Image schema using [BinaryToImage](https://nlp.johnsnowlabs.com/docs/en/ocr_pipeline_components#binarytoimage). More details about Image Schema [here](https://nlp.johnsnowlabs.com/docs/en/ocr_structures#image-schema).
// MAGIC * Recognize text using [ImageToText](https://nlp.johnsnowlabs.com/docs/en/ocr_pipeline_components#imagetotext) transformer.

// COMMAND ----------

def pipeline() = {
    
    // Transforrm binary data to struct image format
    val binaryToImage = new BinaryToImage()
      .setInputCol("content")
      .setOutputCol("image")

    // Run OCR
    val ocr = new ImageToText()
      .setInputCol("image")
      .setOutputCol("text")
      .setConfidenceThreshold(65)
    
    new Pipeline().setStages(Array(
      binaryToImage,
      ocr
    ))
}

// COMMAND ----------

// MAGIC %md ## Download images from public S3 bucket to dbfs

// COMMAND ----------

// MAGIC %sh
// MAGIC OCR_DIR=/dbfs/tmp/ocr
// MAGIC if [ ! -d "$OCR_DIR" ]; then
// MAGIC     mkdir $OCR_DIR
// MAGIC     cd $OCR_DIR
// MAGIC     wget https://s3.amazonaws.com/auxdata.johnsnowlabs.com/public/ocr/datasets/images.zip
// MAGIC     unzip images.zip
// MAGIC fi

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/tmp/ocr/images/"))

// COMMAND ----------

// MAGIC %md ## Read images as binary files from DBFS

// COMMAND ----------

val imagesPath = "/tmp/ocr/images/*.tif"
val imagesExampleDf = spark.read.format("binaryFile").load(imagesPath).cache()
display(imagesExampleDf)

// COMMAND ----------

// MAGIC %md ## Read images as binary files from S3 directly using credentials

// COMMAND ----------

// val ACCESS_KEY = ""
// val SECRET_KEY = ""
// sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", ACCESS_KEY)
// sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", SECRET_KEY)
// val imagesPath = "s3a://dev.johnsnowlabs.com/ocr/datasets/news.2B/0/*.tif"
// val imagesExampleDf = spark.read.format("binaryFile").load(imagesPath).cache()
// display(imagesExampleDf)

// COMMAND ----------

// MAGIC %md ## Display count of images

// COMMAND ----------

imagesExampleDf.count()

// COMMAND ----------

// MAGIC %md ## Preview images using _display_images_ function

// COMMAND ----------

imagesExampleDf.asImage().showImage(limit=3)

// COMMAND ----------

// MAGIC %md ## Run OCR pipelines

// COMMAND ----------

val result = pipeline().fit(imagesExampleDf).transform(imagesExampleDf).cache()

// COMMAND ----------

// MAGIC %md ## Display results

// COMMAND ----------

display(result.select("text", "confidence"))

// COMMAND ----------

// MAGIC %md ## Clear cache

// COMMAND ----------

result.unpersist()
imagesExampleDf.unpersist()
