# DICOM Notebooks Guide

This folder contains Visual NLP / Spark OCR notebooks for working with DICOM data. The examples cover metadata inspection, metadata de-identification, pixel-level redaction, DICOM image rendering, encapsulated PDF workflows, VLM-based OCR, streaming, pretrained pipelines, and a full MIDI-B solution.

Use this README as a routing guide: start with the notebook that matches your use case, then move to the more specialized notebooks when you need a particular implementation pattern.

## Environment-Specific Setup

For Databricks setup instructions, see:

- [Databricks Setup](../../databricks/Readme.md)

For Colab, SageMaker, and local setup instructions, see:

- [Colab / SageMaker / Local Setup](../../sh_install_scripts/README.md)

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

Pixel PHI OCR options:

- [`SparkOcrDicomVLM.ipynb`](SparkOcrDicomVLM.ipynb): latest VLM-based option for handwritten text, printed text, and DICOMs containing PDF packets.
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