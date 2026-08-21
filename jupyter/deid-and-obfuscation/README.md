# 🔏 De-identification & Obfuscation Notebooks Guide

This folder contains Visual NLP notebooks for de-identifying and obfuscating Protected Health Information (PHI) across images, PDFs, and Whole Slide Images (WSI). The examples cover OCR-based PHI detection, NER-driven entity recognition, pixel-level redaction, obfuscation with realistic fake data, pretrained pipelines, context-augmented subentity pipelines, and WSI-specific workflows.

Use this README as a routing guide: start with the notebook that matches your document type and use case, then move to the more specialized notebooks when you need a particular implementation pattern.

## Environment-Specific Setup

For Databricks setup instructions, see:

- [Databricks Setup](../../databricks/Readme.md)

For Colab, SageMaker, and local setup instructions, see:

- [Colab / SageMaker / Local Setup](../../sh_install_scripts/README.md)

## LLM Skill

De-identification & Obfuscation skill that you can download, zip, and provide to Claude, Codex, or Gemini. The skill includes the information from these notebooks along with recommended best practices, so you can ask questions or get help with de-identification and obfuscation tasks.

- [Image & PDF De-Identification Skill](../skill/README.md)

> **Note:** SVS and Image/PDF Obfuscation are not currently covered by this skill and will be added in a future release.

## Quick Notebook Picker

| If you want to... | Start with | Why |
|---|---|---|
| De-identify PHI in scanned images and PDFs | [`DeIdentification.ipynb`](DeIdentification.ipynb) | Remove PHI by drawing a bounding box over detected PHI regions in scanned documents. |
| Obfuscate PHI in scanned images and PDFs | [`Obfuscation.ipynb`](Obfuscation.ipynb) | Remove PHI by rendering obfuscated (synthetic) text in place of PHI in scanned documents. |
| Build a custom deid/obfuscation pipeline using clinical NER | [`AnonymizationPipelineBuilder_Healthcare.ipynb`](AnonymizationPipelineBuilder_Healthcare.ipynb) | Wrap image/PDF stages around state-of-the-art clinical NER pretrained pipelines for quick iteration. |
| Use a pretrained Visual NLP pipeline | [`Pretrained_Pipeline.ipynb`](Pretrained_Pipeline.ipynb) | Drop-in pretrained Visual NLP pipeline for de-identification and obfuscation of images and PDFs. |
| Use the new VLM-based OCR model for de-identification | [`SparkOCRVLMDeIdentification.ipynb`](SparkOCRVLMDeIdentification.ipynb) | Leverages a Vision Language Model for higher-accuracy OCR, particularly for handwritten text and complex layouts. |
| De-identify a folder of WSI files in batch | [`SparkOcrWSIDeidGrundium.ipynb`](SparkOcrWSIDeidGrundium.ipynb) | Extends the single-file WSI workflow to batch-process an entire directory of WSI files. |
| De-identify WSI files that are wrapped in DICOM | [`SparkOcrWSIDeidTcia.ipynb`](SparkOcrWSIDeidTcia.ipynb) | Combines WSI pixel redaction with DICOM metadata de-identification for pathology DICOM (DICOM WSI) files. |

## De-identification vs. Obfuscation

Visual NLP supports two distinct PHI removal strategies that can be used separately or combined:

**De-identification (Redaction)** blanks out detected PHI by drawing filled rectangles over the sensitive region in the image. The original text is visually removed and the structural integrity of the document is preserved.

**Obfuscation (Replacement)** substitutes detected PHI with realistic but entirely synthetic values. A name is replaced with a different fake name, a date is shifted to a plausible alternative, a location is swapped for a fictional one. This mode is preferred for datasets that need to retain statistical or linguistic realism for downstream ML tasks.

## Supported Document Types

| Document Type | De-identification | Obfuscation |
|---|---|---|
| Scanned images (JPEG, PNG, TIFF) | ✅ | ✅ |
| PDF documents (scanned or native) | ✅ | ✅ |
| Whole Slide Images (SVS, NDPI, SCN, etc.) | ✅ | — |
| DICOM WSI (pathology DICOM) | ✅ | — |
