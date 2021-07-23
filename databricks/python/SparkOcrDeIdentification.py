# Databricks notebook source
# MAGIC %md ##De-identification in Spark OCR

# COMMAND ----------

# MAGIC %md ####0 Import libs

# COMMAND ----------

import os
import matplotlib.pyplot as plt
from pyspark.ml import Pipeline, PipelineModel

import sparknlp
import sparknlp_jsl
from sparknlp.base import *
from sparknlp.util import *
from sparknlp.annotator import *
from sparknlp_jsl.base import *
from sparknlp_jsl.annotator import *
from sparknlp.pretrained import ResourceDownloader

import sparkocr
from sparkocr.transformers import *
from sparkocr.utils import *
from sparkocr.enums import *

sparkocr.info()

# COMMAND ----------

# MAGIC %md ###1 Define Spark NLP de-identification pipeline

# COMMAND ----------

def deidentification_nlp_pipeline(input_column, prefix = ""):
    document_assembler = DocumentAssembler() \
        .setInputCol(input_column) \
        .setOutputCol(prefix + "document")

    # Sentence Detector annotator, processes various sentences per line
    sentence_detector = SentenceDetector() \
        .setInputCols([prefix + "document"]) \
        .setOutputCol(prefix + "sentence")

    tokenizer = Tokenizer() \
        .setInputCols([prefix + "sentence"]) \
        .setOutputCol(prefix + "token")

    # Clinical word embeddings
    word_embeddings = WordEmbeddingsModel.pretrained("embeddings_clinical", "en", "clinical/models") \
        .setInputCols([prefix + "sentence", prefix + "token"]) \
        .setOutputCol(prefix + "embeddings")
    # NER model trained on i2b2 (sampled from MIMIC) dataset
    clinical_ner = MedicalNerModel.pretrained("ner_deid_large", "en", "clinical/models") \
        .setInputCols([prefix + "sentence", prefix + "token", prefix + "embeddings"]) \
        .setOutputCol(prefix + "ner")

    custom_ner_converter = NerConverter() \
        .setInputCols([prefix + "sentence", prefix + "token", prefix + "ner"]) \
        .setOutputCol(prefix + "ner_chunk") \
        .setWhiteList(['NAME', 'AGE', 'CONTACT', 'LOCATION', 'PROFESSION', 'PERSON', 'DATE'])

    nlp_pipeline = Pipeline(stages=[
            document_assembler,
            sentence_detector,
            tokenizer,
            word_embeddings,
            clinical_ner,
            custom_ner_converter
        ])
    empty_data = spark.createDataFrame([[""]]).toDF(input_column)
    nlp_model = nlp_pipeline.fit(empty_data)
    return nlp_model

# COMMAND ----------

# MAGIC %md ###2 Define Spark OCR pipeline

# COMMAND ----------

# Read Pdf as image
pdf_to_image = PdfToImage()\
  .setInputCol("content")\
  .setOutputCol("image_raw")\
  .setResolution(400)

# Extract text from image
ocr = ImageToText() \
    .setInputCol("image_raw") \
    .setOutputCol("text") \
    .setIgnoreResolution(False) \
    .setPageIteratorLevel(PageIteratorLevel.SYMBOL) \
    .setPageSegMode(PageSegmentationMode.SPARSE_TEXT) \
    .setWithSpaces(True) \
    .setConfidenceThreshold(70)

# Found coordinates of sensitive data
position_finder = PositionFinder() \
    .setInputCols("ner_chunk") \
    .setOutputCol("coordinates") \
    .setPageMatrixCol("positions") \
    .setMatchingWindow(100) \
    .setPadding(1)

# Draw filled rectangle for hide sensitive data
drawRegions = ImageDrawRegions()  \
    .setInputCol("image_raw")  \
    .setInputRegionsCol("coordinates")  \
    .setOutputCol("image_with_regions")  \
    .setFilledRect(True) \
    .setRectColor(Color.black)
    
# OCR pipeline
deid_pipeline = PipelineModel(stages=[
    pdf_to_image,
    ocr,
    deidentification_nlp_pipeline(input_column="text"),
    position_finder,
    drawRegions
])

# COMMAND ----------

# MAGIC %md ###3 Prepare documents

# COMMAND ----------

# MAGIC %sh
# MAGIC OCR_DIR=/dbfs/tmp/deid_ocr3
# MAGIC if [ ! -d "$OCR_DIR" ]; then
# MAGIC     mkdir $OCR_DIR
# MAGIC     cd $OCR_DIR
# MAGIC     for i in {0..3}
# MAGIC     do
# MAGIC       wget https://raw.githubusercontent.com/JohnSnowLabs/spark-nlp-workshop/master/tutorials/Certification_Trainings/Healthcare/data/ocr/MT_0$i.pdf
# MAGIC     done
# MAGIC fi

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/tmp/deid_ocr3/"))

# COMMAND ----------

file_path='dbfs:/tmp/deid_ocr3/*.pdf'
pdfs = spark.read.format("binaryFile").load(file_path)
pdfs.show()

# COMMAND ----------

from sparkocr.databricks import display_images
display_images(PdfToImage().transform(pdfs).limit(2))

# COMMAND ----------

# MAGIC %md ###4 Run pipeline

# COMMAND ----------

deid_results = deid_pipeline.transform(pdfs).cache()

# COMMAND ----------

# MAGIC %md #### Display Ner chuncs 

# COMMAND ----------

display(deid_results.select("ner_chunk").limit(2))

# COMMAND ----------

# MAGIC %md ####Display coordinates of text which need to hide

# COMMAND ----------

display(deid_results.select('coordinates').limit(2))

# COMMAND ----------

# MAGIC %md ###4 Display original and de-identified results

# COMMAND ----------

r = deid_results.select("image_raw", "image_with_regions").collect()[0]
img_orig = r.image_raw
img_deid = r.image_with_regions

img_pil_orig = to_pil_image(img_orig, img_orig.mode)
img_pil_deid = to_pil_image(img_deid, img_deid.mode)

plt.figure(figsize=(24,16))
plt.subplot(1, 2, 1)
plt.imshow(img_pil_orig, cmap='gray')
plt.title('original')
plt.subplot(1, 2, 2)
plt.imshow(img_pil_deid, cmap='gray')
plt.title("de-id'd")
plt.show()
