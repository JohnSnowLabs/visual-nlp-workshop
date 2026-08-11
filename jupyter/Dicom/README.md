# DICOM Notebooks Guide

This folder contains Visual NLP / Spark OCR notebooks for working with DICOM data. The examples cover metadata inspection, metadata de-identification, pixel-level redaction, DICOM image rendering, encapsulated PDF workflows, VLM-based Dicom de-identification, streaming, pretrained pipelines, and a full solution for the [MIDI-B challenge dataset](https://www.cancerimagingarchive.net/collection/midi-b-test-midi-b-validation/).

Use this README as a routing guide: start with the notebook that matches your use case, then move to the more specialized notebooks when you need a particular implementation pattern.

## Environment-Specific Setup

For Databricks setup instructions, see:

- [Databricks Setup](../../databricks/Readme.md)

For Colab, SageMaker, and local setup instructions, see:

- [Colab / SageMaker / Local Setup](../../sh_install_scripts/README.md)

## LLM Skill

DICOM skill that you can download, zip, and provide to Claude, Codex, or Gemini. The skill includes the information from these notebooks along with recommended best practices, so you can ask questions or get help with DICOM-related tasks.

- [DICOM Skill](./skill/README.md)

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
| Pipeline Builder Clinical NER Models | [`SparkOcrDicomPipelineBuilder.ipynb`](SparkOcrDicomPipelineBuilder.ipynb) | Latest notebook to help wrap dicom pipelines around state of the Healthcare clinical pretrained pipelines. |

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
| Dicom Pipeline Builder | [`SparkOcrDicomPipelineBuilder.ipynb`](SparkOcrDicomPipelineBuilder.ipynb) | Latest notebook to help wrap dicom pipelines around state of the Healthcare clinical pretrained pipelines. |
| Metadata de-identification | [`SparkOcrDicomMetadata.ipynb`](SparkOcrDicomMetadata.ipynb) | Latest DICOM-to-metadata notebook for extracting and preparing metadata fields, including tags that can be routed into de-identification flows. |
| PHI identification in DICOM images | [`SparkOcrDicomToImageV3.ipynb`](SparkOcrDicomToImageV3.ipynb), [`SparkOcrDicomVLM.ipynb`](SparkOcrDicomVLM.ipynb), [`SparkOcrDicomDrawRegions.ipynb`](SparkOcrDicomDrawRegions.ipynb) | Use `DicomToImageV3` to extract pixels into images, the VLM notebook for VLM-based OCR/PHI identification, and Draw Regions to render detected regions back onto the DICOM. |
| Encapsulated PDF de-identification | [`SparkOcrDeidentificationDicomWithEncapsulatedPDF.ipynb`](SparkOcrDeidentificationDicomWithEncapsulatedPDF.ipynb) | Latest notebook for DICOM files that contain encapsulated PDFs. |
| MIDI-B solution | [`SparkOcrMIDIBSolution.ipynb`](SparkOcrMIDIBSolution.ipynb) | Use this when you want to run the JSL solution on the MIDI-B dataset. |

## AWS Marketplace listings
Some predefined pipelines are made accessible through AWS Marketplace as Sagemaker products,

[Dicom Images De-identification - Full](https://aws.amazon.com/marketplace/pp/prodview-jb2mn4ionsi2s): this advanced pipeline eliminates all visible text within DICOM images and removes or anonymizes most metadata fields, including patient identifiers, physician details, and hospital information.
</br>

[DICOM Images De-identification - Alias](https://aws.amazon.com/marketplace/pp/prodview-uqh2xim2fcbxa): this pipeline offers a cutting-edge solution for healthcare data scientists focused on data privacy and adherence to health regulations, effectively masking PHI information in DICOM images, by replacing personal identifiers with pseudonyms instead of removing them, ensuring that PHI is no longer traceable while maintaining data integrity for longitudinal studies and collaborations.
</br>

[DICOM Images De-identification - Base](https://aws.amazon.com/marketplace/pp/prodview-y6of2kcxqt7ta): this pipeline enables automated PHI redaction in DICOM images, ensuring compliance with HIPAA and other healthcare privacy regulations. The model performs the least intrusive form of DICOM de-identification removing only the most critical PHI from images and most essential metadata fields while preserving all non-sensitive details for research and analysis. 
</br>

## Performance & Benchmarks
We encourage the reader the review our [dicom benchmarks](https://nlp.johnsnowlabs.com/docs/en/ocr_benchmark#dicom-de-identification-benchmark) for different platforms and pipelines.


## Other Resources

| Resource | Description |
|---|---|
| [DICOM Paper](https://link.springer.com/chapter/10.1007/978-3-032-26211-0_12) | Research paper covering DICOM de-identification methodology and results. |
| [DICOM Blogpost](https://medium.com/john-snow-labs/de-identifying-dicom-files-a-step-by-step-guide-with-john-snow-labs-visual-nlp-2c21b60f92a8) | Step-by-step guide for de-identifying DICOM files with John Snow Labs Visual NLP. |
| [DICOM Repo](https://github.com/JohnSnowLabs/dicom-deid-dataset) | Public dataset, benchmark results, and comparison materials. |
| [DICOM Databricks Benchmarks](https://medium.com/john-snow-labs/de-identifying-dicom-files-a-step-by-step-guide-with-john-snow-labs-visual-nlp-2c21b60f92a8#:~:text=MIDI%2DB%20Subset.-,Databricks%20Speed%20Benchmarks,-To%20evaluate%20processing) | Databricks-specific speed benchmark results for DICOM pixel and metadata de-identification. |

</br>
[^1]: try, for instance AWS' c7a family. 
