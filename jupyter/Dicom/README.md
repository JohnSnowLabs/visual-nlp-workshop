if we had to prepare a landing page README.md for the jupyter/Dicom folder, to help users to navigate it, how would you organize it? one idea is to do it across topics, like,
 
+ Basic Getting Started pipelines.
+ Blanket, all texts, deid.
+ PHI, just entities, deid.
+ Estimating infra costs: link to benchmarks.




# DICOM Notebooks Guide

This folder contains Visual NLP / Spark OCR notebooks for working with DICOM data. The examples cover metadata inspection, metadata de-identification, pixel-level redaction, DICOM image rendering, encapsulated PDF workflows, VLM-based Dicom de-identification, streaming, pretrained pipelines, and a full solution for the [MIDI-B challenge dataset](https://www.cancerimagingarchive.net/collection/midi-b-test-midi-b-validation/).

Use this README as a routing guide: start with the notebook that matches your use case, then move to the more specialized notebooks when you need a particular implementation pattern.

## Quick Notebook Picker

| If you want to... | Start with | Why |
|---|---|---|
| Extract and inspect DICOM metadata | [`SparkOcrDicomMetadata.ipynb`](SparkOcrDicomMetadata.ipynb) | Focused `DicomToMetadata` examples, including VR filtering and `cleanTag` extraction for NER based PHI cleaning for free text. |
| De-identify metadata only | [`SparkOcrMetadataDeIdentification.ipynb`](SparkOcrMetadataDeIdentification.ipynb) | Primary metadata workflow with strategy files, private tag removal, mappings, group rules, NER cleaning, and UID/patient mapping extraction. |
| De-identify PHI in image pixels | [`SparkOcrDicomToImageV3.ipynb`](SparkOcrDicomToImageV3.ipynb), an OCR option below, then [`SparkOcrDicomDrawRegions.ipynb`](SparkOcrDicomDrawRegions.ipynb) | Extract pixels into images, identify PHI, then render redactions back onto the DICOM. |
| MIDI-B | [`SparkOcrMIDIBSolution.ipynb`](SparkOcrMIDIBSolution.ipynb) | End-to-end MIDI-B-oriented solution combining metadata and pixel de-identification. |
| Convert or render DICOM images | [`SparkOcrDicomToImageV3.ipynb`](SparkOcrDicomToImageV3.ipynb) | Best entry point for `DicomToImageV3`, frame sampling, scaling, and compression. |
| Draw or redact regions on DICOMs | [`SparkOcrDicomDrawRegions.ipynb`](SparkOcrDicomDrawRegions.ipynb) | Shows detection-only masking, OCR/NER-based coordinates, image previews, and final DICOM redaction. |
| Remove all detected image text | [`SparkOcrDicomRemoveText.ipynb`](SparkOcrDicomRemoveText.ipynb) | Smaller legacy example for broad text removal from DICOM images. |
| De-identify encapsulated PDFs inside DICOM | [`SparkOcrDeidentificationDicomWithEncapsulatedPDF.ipynb`](SparkOcrDeidentificationDicomWithEncapsulatedPDF.ipynb) | Extracts PDF from DICOM, OCRs pages, redacts PHI, rebuilds the PDF, and updates the DICOM. |
| Use VLM OCR for DICOM text detection | [`SparkOcrDicomVLM.ipynb`](SparkOcrDicomVLM.ipynb) | Uses 1B VLM for OCR with coordinates, then supports blanket or PHI-only redaction. |
| Run de-identification in streaming mode | [`SparkOcrDicomDeIdentificationV2Streaming.ipynb`](SparkOcrDicomDeIdentificationV2Streaming.ipynb) | Spark Structured Streaming example based on the V2 image/OCR pipeline. |
| Try pretrained de-identification pipelines | [`SparkOcrDicomPretrainedPipelines.ipynb`](SparkOcrDicomPretrainedPipelines.ipynb) | Compares ready-made minimal, full anonymization, and pseudonymization DICOM pipelines. |

## Tags & Strategy Files
[Dicom Tags](https://www.dicomlibrary.com/dicom/dicom-tags/), encoded in the header of Dicom files, are a key/value data structure that may contain PHI. To handle PHI removal in Dicom Tags, Visual NLP relies on the Strategy Files.
Strategy Files enumerate a list of `actions` targeted for a specific tag or group of tags. These actions will do things like replacing a name with a pseudonym, or randomizing a date. For an exhaustive list check this [list of actions in strategy files](strategy_actions.md).

## Pixel PHI OCR options
Here we list notebooks according to how they extract text from the image.

- [`SparkOcrDicomVLM.ipynb`](SparkOcrDicomVLM.ipynb): latest VLM-based option for handwritten text, printed text, and DICOMs containing PDF packets(PDF documents embedded into the Dicom). This can deliver acceptable performance in both CPU[^1] and GPU.
- [`SparkOcrDicomDeIdentificationV3.ipynb`](SparkOcrDicomDeIdentificationV3.ipynb): CPU-based option for printed text.
- [`SparkOcrDicomDeIdentificationV2.ipynb`](SparkOcrDicomDeIdentificationV2.ipynb): GPU-based option for printed text and simple handwritten text; use the VLM notebook for complex handwriting.

## Latest Notebooks

These are the latest notebooks to use for the main DICOM workflows in this folder.

| Use case | Latest notebook(s) | Notes |
|---|---|---|
| Metadata de-identification | [`SparkOcrDicomMetadata.ipynb`](SparkOcrDicomMetadata.ipynb) | Latest DICOM-to-metadata notebook for extracting and preparing metadata fields, including tags that can be routed into de-identification flows. |
| PHI identification in DICOM images | [`SparkOcrDicomToImageV3.ipynb`](SparkOcrDicomToImageV3.ipynb), [`SparkOcrDicomVLM.ipynb`](SparkOcrDicomVLM.ipynb), [`SparkOcrDicomDrawRegions.ipynb`](SparkOcrDicomDrawRegions.ipynb) | Use `DicomToImageV3` to extract pixels into images, the VLM notebook for VLM-based OCR/PHI identification, and Draw Regions to render detected regions back onto the DICOM. |
| Encapsulated PDF de-identification | [`SparkOcrDeidentificationDicomWithEncapsulatedPDF.ipynb`](SparkOcrDeidentificationDicomWithEncapsulatedPDF.ipynb) | Latest notebook for DICOM files that contain encapsulated PDFs. |
| MIDI-B solution | [`SparkOcrMIDIBSolution.ipynb`](SparkOcrMIDIBSolution.ipynb) | Use this when you want to run the JSL solution on the MIDI-B dataset. |

## Common Setup

Most notebooks follow the same setup pattern:

### Variables

Define the variables from your license file before running a notebook:

```python
# Required variables from your license file:
license = "..."
secret = "..."          # Visual Product Secret
nlp_secret = "..."      # Healthcare Product Secret
public_version = "..."  # Open Source Version
aws_access_key = "..."
aws_secret_key = "..."
spark_ocr_jar_path = None
```

Many notebooks include `spark_ocr_jar_path` for internal/local testing. In normal licensed environments, the `secret` and `nlp_secret` values are usually the important configuration inputs.

### Install Visual NLP

```python
ocr_version = secret.split("-")[0]

!pip install --upgrade -q spark-ocr==$ocr_version \
  --extra-index-url=https://pypi.johnsnowlabs.com/$secret \
  --upgrade
```

### Install Healthcare NLP

```python
jsl_version = nlp_secret.split("-")[0]

!pip -q install --upgrade spark-nlp-jsl==$jsl_version \
  --extra-index-url https://pypi.johnsnowlabs.com/$nlp_secret
```

### Install Open-Source Spark NLP and PySpark

Check your license file for the correct open-source Spark NLP version.

```python
!pip install --upgrade -q pyspark==3.4.0 spark-nlp==$public_version
```

### Install Java 8

```python
!apt-get update
!apt-get install -y openjdk-8-jdk
```

## 🔴 WARNING: Restart Your Notebook Session

> Restart the notebook session after installing JSL components, before starting Spark OCR.

### Start Spark OCR

```python
from sparkocr import start
import os

if license:
    os.environ["JSL_OCR_LICENSE"] = license
    os.environ["SPARK_NLP_LICENSE"] = license

if aws_access_key:
    os.environ["AWS_ACCESS_KEY"] = aws_access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_key

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

jsl_version = nlp_secret.split("-")[0]
ocr_version = secret.split("-")[0]

extra_configurations = {
    "spark.extraListeners": "com.johnsnowlabs.license.LicenseLifeCycleManager"
}

spark = start(
    secret=secret,
    nlp_secret=nlp_secret,
    jar_path=spark_ocr_jar_path,
    nlp_internal=jsl_version,
    extra_conf=extra_configurations
)

spark
```


[^1]: try, for instance AWS' c7a family. 
