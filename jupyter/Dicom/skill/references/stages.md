# Stage Catalog

Use this file to understand stage contracts. Do not copy full pipeline code from here; copy pipeline code from the selected route-specific `template_file` in `routing.yaml`.

## Input And Keep Rules

- Always read DICOM files into a DataFrame before pipeline code: `dicom_df = spark.read.format("binaryFile").load(dicom_path)`.
- The first DICOM stage in generated metadata and pixel de-identification starter flows must consume `content`; encapsulated PDF uses the grounded PDF template.
- Pixel workflows should start with an input DataFrame that has both `content` and `path`.
- If a DICOM stage consumes `content`, use `setKeepInput(False)` unless a later stage in the same pipeline still needs `content`.
- Never drop `path`. If a DICOM stage consumes `path`, always use `setKeepInput(True)`.
- If a DICOM stage creates a DICOM bytes output column, preserve that bytes column for display, saving, comparison, or downstream stages.
- Exception: when `DicomMetadataDeidentifier` immediately follows `DicomDrawRegions`, it consumes `dicom_pixel_cleaned`, outputs `dicom_metadata_cleaned`, and can use `setKeepInput(False)` because `dicom_metadata_cleaned` replaces `dicom_pixel_cleaned`.
- `DicomDrawRegions` always consumes `path`; `DicomToImageV3` should drop heavy `content` bytes after image extraction.
- `DicomDrawRegions` is an aggregation stage. After it runs, assume upstream intermediate columns are gone.
- `DicomMetadataDeidentifier` consumes `["dicom_pixel_cleaned"]` when it immediately follows `DicomDrawRegions` in pixel plus metadata workflows.
- Metadata-only and cleanTag `DicomMetadataDeidentifier` should consume `["path"]` in starter examples and output `dicom_metadata_cleaned`.
- `DicomToMetadata` may consume `path`, `content`, `dicom_pixel_cleaned`, `dicom_metadata_cleaned`, or the current DICOM bytes column.

## Canonical DICOM Byte Flows

Use these flows when deciding inputs and `setKeepInput(...)`:

```yaml
metadata_only:
  input: path + content
  original_metadata: DicomToMetadata(content) -> metadata_original -> keepInput false
  metadata_deid: DicomMetadataDeidentifier(path) -> dicom_metadata_cleaned -> keepInput true
  cleaned_metadata: DicomToMetadata(dicom_metadata_cleaned) -> metadata_cleaned -> keepInput true when final bytes must remain available

pixel_only:
  input: path + content
  image: DicomToImageV3(content) -> image -> keepInput false
  redaction: DicomDrawRegions(path + coordinates/text_regions) -> dicom_pixel_cleaned -> keepInput true
  validation: display_dicom(dicom_pixel_cleaned)

pixel_plus_metadata:
  input: path + content
  original_metadata: DicomToMetadata(content) -> metadata_original -> keepInput true only if DicomToImageV3 still needs content in the same pipeline
  image: DicomToImageV3(content) -> image -> keepInput false
  redaction: DicomDrawRegions(path + coordinates) -> dicom_pixel_cleaned -> keepInput true
  metadata_deid: DicomMetadataDeidentifier(dicom_pixel_cleaned) -> dicom_metadata_cleaned -> keepInput false
  cleaned_metadata: DicomToMetadata(dicom_metadata_cleaned) -> metadata_cleaned -> keepInput true when final bytes must remain available
  comparison: build_metadata_comparison_df(result)

metadata_clean_tag_ner:
  input: path + content
  extraction: DicomToMetadata(content) -> metadata_original + tag_text + tag_mapping -> keepInput false
  ner: build_stacked_zero_shot_metadata_pipeline(tag_text) -> deid_documents
  metadata_deid: DicomMetadataDeidentifier(path + tag_mapping + deid_documents) -> dicom_metadata_cleaned -> keepInput true
  cleaned_metadata: DicomToMetadata(dicom_metadata_cleaned) -> metadata_cleaned -> keepInput true when final bytes must remain available
```

Encapsulated PDF workflows are grounded in `template-encapsulated-pdf.md`; do not rewrite their DICOM input columns from memory.

## DicomToMetadata

Role: Extract metadata and, when needed, free-text metadata values for cleanTag NER.

Key params:
- `setInputCol(...)`: required. Use `content` for the first/original metadata snapshot, and use `dicom_metadata_cleaned` for final metadata validation after metadata de-id.
- `setOutputCol(...)`: use `metadata_original` for original metadata and `metadata_cleaned` for cleaned metadata.
- `setKeepInput(...)`: use `False` for standalone original `content` extraction; use `True` if a later stage still needs the input column or if preserving the final DICOM bytes column after metadata extraction.
- `setExtractTagForNer(...)`: skill value for cleanTag workflows is `True`; use `False` for normal inspection/validation.
- `setTagMappingCol("tag_mapping")`: default mapping column for cleanTag.
- `setTagCol("tag_text")`: free-text metadata column for NER.

## DicomMetadataDeidentifier

Role: Rewrite DICOM metadata using strategy actions and optional cleanTag NER output.

Key params:
- `setInputCols(...)`: required. Use `["path"]` for metadata-only and cleanTag starter examples; use `["dicom_pixel_cleaned"]` after pixel redaction.
- `setOutputCol("dicom_metadata_cleaned")`: required metadata de-id output.
- `setKeepInput(...)`: library default is `False`; use `True` when input is `path`. In pixel plus metadata workflows, keep `False` because `dicom_metadata_cleaned` replaces `dicom_pixel_cleaned`.
- `setRemovePrivateTags(False)`: skill default.
- `setStrategyFile(strategy_file_path)`: default file-backed tag strategy. Create the CSV with `textwrap.dedent`, write it to disk, and pass the path.
- `._set(strategyFileContent=csv_strategy_data)`: alternate in-memory tag strategy CSV.
- `setTagMappingCol("tag_mapping")`: skill default cleanTag mapping column.
- `setTagCleanedCol("deid_documents")`: cleanTag NLP output.
- `setExternalMappingCol("external_mapping")`: library default external mapping column for `replaceWithMapping`; the DataFrame must be built with `dicomExternalSchema` from `sparkocr.schemas`.
- `setGroupStrategyFile(group_strategy_file_path)`: optional group strategy; library default is `None`. Create the CSV with `textwrap.dedent`, write it to disk, and pass the path.
- `._set(groupStrategyFileContent=csv_group_strategy_data)`: alternate in-memory group strategy CSV.
- `ignoreVR`: default `"OB,OW,UN,OF,OD,OL,UT"`.

## DicomToImageV3

Role: Render DICOM pixel frames into images for OCR or text detection.

Key params:
- `setInputCols(["content"])`: required.
- `setOutputCol("image")`: required.
- `setKeepInput(False)`: drop heavy content bytes.
- `setMemoryOptimized(False)`: skill default.
- `setScale(scale)`: define explicitly when coordinates need scaling.
- `setFrameLimit(frame_sampling)`: frame count/sampling limit.
- `setFrameSamplingStrategy(FrameSamplingStrategy.CONSECUTIVE)`: skill default/example value; available values are `STRIDE`, `RANDOM`, `MIDDLE`, `CONSECUTIVE`.
- `setFrameDimsCol("frame_dims")`: required when VLM is used.
- `setCompressImage(True)`: required when VLM is used; changes image schema behavior and is not compression.
- `setCompressImage(False)`: required when VLM is not used.
- `setCompressionMode(...)`: actual compression mode. Values: `disabled`, `enabled`, `auto`.
- `setCompressionQuality(80)`: actual compression quality example.
- `setCompressionThreshold(1)`: `auto` compresses when megapixels are greater than threshold.

## VLM Image Extraction Rule

When VLM is used for OCR, always set compressed image schema and dimensions on the image extraction or handling stage:

```python
BinaryToImage().setCompressImage(True).setImageDimsCol("frame_dims")
PdfToImage().setCompressImage(True).setImageDimsCol("frame_dims")
DicomToImageV3().setCompressImage(True).setFrameDimsCol("frame_dims")
```

When VLM is not used, set `setCompressImage(False)` on the image extraction stage.

## DicomDrawRegions

Role: Draw redaction regions into DICOM pixels and return DICOM bytes.

Key params:
- `setInputCol("path")`: required.
- `setInputRegionsCol("coordinates")` or `setInputRegionsCol("text_regions")`: required.
- `setOutputCol("dicom_pixel_cleaned")`: required.
- `setAggCols(["path"])`: skill default.
- `setKeepInput(True)`: use because input is `path`; default stage value is `False`.
- `setScaleFactor(1 / scale)`: use when images were scaled before coordinate detection; library default is `1`.
- `setOutputPartitionSize(1)`: library default.
- `setColor(Color.BLACK)`: library default color is black.

Inspection rule:
- If `DicomDrawRegions` is the last stage, use `display_dicom(df=result, fields="dicom_pixel_cleaned", limit=1, width=300)`.
- Do not select upstream intermediate columns such as `image`, `text`, `regions`, or `coordinates` from a DataFrame after `DicomDrawRegions`.
- If downstream stages run after `DicomDrawRegions`, such as `DicomMetadataDeidentifier` or `DicomToMetadata`, their output columns may be selected because they are produced after the aggregation.
- When the user wants OCR text, regions, or coordinates, create a separate inspection pipeline that stops before `DicomDrawRegions`.

## MedicalVisionLLM

Role: Highest-accuracy OCR/VLM text detection and recognition.

Contract:
- Pretrained call must be one line: `MedicalVisionLLM.pretrained("jsl-ocr-gguf-vlm1", "en", "clinical/ocr")`.
- Inputs: `caption_document`, `image_assembler`.
- Output: `completions`.
- Recommended on GPU.
- Always use this full parameter block:

```python
vlm_ocr = MedicalVisionLLM.pretrained("jsl-ocr-gguf-vlm1", "en", "clinical/ocr") \
    .setInputCols(["caption_document", "image_assembler"]) \
    .setOutputCol("completions") \
    .setNGpuLayers(99) \
    .setNCtx(32768) \
    .setNParallel(1) \
    .setNBatch(2048) \
    .setNUbatch(1024) \
    .setNPredict(1024) \
    .setTemperature(0.01) \
    .setTopK(1) \
    .setTopP(1.0) \
    .setRepeatPenalty(1.03) \
    .setRepeatLastN(256) \
    .setMinKeep(0) \
    .setNProbs(0) \
    .setBatchSize(1) \
    .setDisableLog(False)
```
- Follow with `DocumentCoordinatesToText` to produce `text`, `positions`, and `regions`.

## DocumentCoordinatesToText

Role: Convert VLM coordinate completions into OCR text and coordinate columns.

Key params:
- `setInputCol("completions")`: required.
- `setImageDimsCol("frame_dims")`: required.
- `setOutputCol("text")`: required.
- `setPageMatrixCol("positions")`: required for `PositionFinder`.
- `setRegionCol("regions")`: VLM-detected regions.
- `setLineTolerance(5)`: skill example override; library default is `15`.

## ImageTextDetector And ImageTextDetectorV2

Role: Text detection only. They do not recognize text.

Model notes:
- `ImageTextDetector` is Scala based and can run CPU/GPU.
- `ImageTextDetectorV2` is Python based and can run CPU/GPU.
- Both can work with `ImageToTextV2` and `ImageToTextV3`.

Contract:
- For `ImageTextDetector`, use one-line pretrained call: `ImageTextDetector.pretrained("image_text_detector_mem_opt", "en", "clinical/ocr")`.
- For `ImageTextDetectorV2`, use one-line pretrained call: `ImageTextDetectorV2.pretrained("image_text_detector_v2", "en", "clinical/ocr")`.
- For blanket visible-text redaction, `ImageTextDetector` and `ImageTextDetectorV2` are both valid before `DicomDrawRegions`.
- Input: `image`.
- Output: `text_regions`.
- Both detector options can run CPU/GPU; wire GPU behavior from a named config key when the selected detector supports `.setUseGPU(...)`.
- Keep the selected detector in a config value, such as `config["text_detector"]`.
- Default to `ImageTextDetector`; switch to `ImageTextDetectorV2` when the user asks for the Python-based detector.
- Configure threshold/refiner values through named config keys instead of hard-coding them in the stage.

## ImageToTextV2

Role: Printed OCR recognizer with CPU/GPU support.

Contract:
- Pretrained call must be one line: `ImageToTextV2.pretrained("ocr_large_printed_v2_opt", "en", "clinical/ocr")`.
- Inputs: `image` and detector regions.
- Output: `text`.
- Use `setOutputFormat(OcrOutputFormat.TEXT_WITH_POSITIONS)` for coordinate workflows.

## ImageToTextV3

Role: CPU-only printed OCR recognizer.

Contract:
- Inputs: `image`, `text_regions`.
- Output: `text`.
- Use when the user asks for the cheaper CPU path.

## PositionFinder

Role: Convert NLP chunks into image/DICOM coordinates.

Key params:
- `setInputCols([ner_output])`: only the chunk column, not `text`.
- `setOutputCol("coordinates")`: required.
- `setPageMatrixCol("positions")`: required for VLM coordinate output.
- `setIgnoreSchema(False)`: library default.
- `setOcrScaleFactor(1.1)`: skill default/example value.
- `setSmoothCoordinates(True)`: optional; library default is `False`.

## NER External References

These are external Spark NLP and Healthcare NLP API links. If a user is confused about, asks about, compares, or requests parameters for any NER-related stage, use the relevant external link before answering. Do not guess NER stage behavior, parameters, defaults, or compatibility. DICOM column contracts in this skill remain the source of truth for generated DICOM pipeline code because they are grounded in local workshop examples.

| Stage | External reference | Skill usage note |
|---|---|---|
| `DocumentAssembler` | [Spark NLP DocumentAssembler](https://sparknlp.org/api/python/reference/autosummary/sparknlp/base/document_assembler/index.html) | Start every NER subpipeline from raw OCR or metadata text. |
| `SentenceDetector` | [Spark NLP SentenceDetector](https://sparknlp.org/api/python/reference/autosummary/sparknlp/annotator/sentence/sentence_detector/index.html) | Use normal sentence detection for pixel/PDF OCR text; use custom `<dicom>` bounds for metadata cleanTag. |
| `Tokenizer` | [Spark NLP Tokenizer](https://sparknlp.org/api/python/reference/autosummary/sparknlp/annotator/token/tokenizer/index.html) | Tokenize sentence annotations before NER. |
| `PretrainedZeroShotNER` | [Healthcare NLP PretrainedZeroShotNER](https://nlp.johnsnowlabs.com/licensed/api/python/reference/autosummary/sparknlp_jsl/annotator/ner/pretrained_zero_shot_ner/index.html) | Use as a stackable stage-level model for configurable PHI detection; do not use it inside the DICOM pipeline builder. |
| `NerConverterInternal` | [Healthcare NLP NerConverterInternal](https://nlp.johnsnowlabs.com/licensed/api/python/reference/autosummary/sparknlp_jsl/annotator/ner/ner_converter_internal/index.html) | Convert NER tags into chunk columns before `ChunkMergeApproach` or downstream de-identification. |
| `ChunkMergeApproach` | [Healthcare NLP ChunkMerge](https://nlp.johnsnowlabs.com/licensed/api/python/reference/autosummary/sparknlp_jsl/annotator/merge/chunk_merge/index.html) | Merge multiple chunk columns into one final chunk column; external docs may show `ChunkMergeApproach`, but generated examples should keep the notebook-grounded `ChunkMergeApproach()` pattern unless the user asks otherwise. |
| `DeIdentification` | [Healthcare NLP DeIdentification](https://nlp.johnsnowlabs.com/licensed/api/python/reference/autosummary/sparknlp_jsl/annotator/deid/deIdentification/index.html) | Use for metadata cleanTag workflows to produce `deid_documents` from sentence, token, and chunk inputs. |

## Healthcare NLP Subpipelines

Role: Detect and de-identify PHI in OCR text or metadata free text.

Contract:
- Wrap NER subpipelines in a function.
- Start with `DocumentAssembler`.
- Fit on an empty DataFrame.
- Return a fitted `PipelineModel`.
- For pixel PHI, downstream `PositionFinder` should consume only the final chunk output.
- For metadata cleanTag, final NLP output must be `deid_documents`.
- For metadata cleanTag, use `SentenceDetector` with `.setCustomBounds(["<dicom>"])` and `.setUseCustomBoundsOnly(True)`, then feed `DeIdentification`; intermediate stage output names can vary as long as inputs are wired correctly.

## Encapsulated PDF Stages

Architecture: `DicomToPdf -> PdfToImage -> OCR/VLM -> build_stacked_zero_shot_ner_pipeline -> PositionFinder -> ImageSchemaConverter -> ImageDrawRegions -> ImageToPdf -> DicomUpdatePdf`.

Contracts:
- Use `DicomToPdf` to extract the encapsulated PDF from DICOM.
- Use `DicomToPdf.setInputCols(["path"]).setKeepInput(True)`; this is grounded in the encapsulated PDF notebooks.
- Use `PdfToImage` to render PDF pages to `image`.
- Use VLM OCR on PDF page images when highest accuracy is requested.
- Use `build_stacked_zero_shot_ner_pipeline(input_text="text", output_chunk="merged_ner_chunk")` for encapsulated PDF PHI detection after OCR produces `text`.
- Convert assembler image schema back to internal before `ImageDrawRegions`.
- Use `ImageDrawRegions` for PDF page redaction, not `DicomDrawRegions`.
- Use `ImageToPdf` to rebuild PDF pages.
- Use `DicomUpdatePdf` to write the cleaned PDF back into DICOM; output is `dicom`.
- Use `DicomUpdatePdf.setInputCol("path").setKeepInput(True)` so `path` is not dropped.

## Utility Functions

- Use this input pattern in generated examples before any pipeline transform:

```python
dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)
```

- Use `display_dicom(df=result, fields="<dicom_col>", limit=1, width=300)` for DICOM pipeline validation.
- Use `display_images` only when the user explicitly wants to inspect image content.
- To save DICOM bytes to disk, use `save_dicom_to_disk(...)` with the final DICOM bytes column. Keep this utility simple: create the save directory if needed, extract the DICOM bytes, keep the same base filename from `path`, save to disk, and return saved paths.
- Use `build_metadata_df(result, metadata_col="metadata_original")` for single metadata column inspection.
- Use `build_metadata_comparison_df(result)` after metadata de-identification pipelines that output `metadata_original` and `metadata_cleaned`.
- Outside single metadata inspection and intentional intermediate inspection pipelines, prefer `display_dicom(...)` over post-transform `result.select(...)` examples.

```python
def save_dicom_to_disk(dataframe, dicom_col="dicom_metadata_cleaned", output_dir="/tmp/dicom_deid"):
    from pathlib import Path

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    saved_paths = []

    for row in dataframe.select("path", dicom_col).toLocalIterator():
        base_file_name = Path(row["path"]).name
        target_path = output_path / base_file_name
        dicom_bytes = row[dicom_col]
        if isinstance(dicom_bytes, bytearray):
            dicom_bytes = bytes(dicom_bytes)
        with open(target_path, "wb") as f:
            f.write(dicom_bytes)
        saved_paths.append(str(target_path))

    return saved_paths

def build_metadata_df(dataframe, metadata_col="metadata_original"):
    import json
    import pandas as pd

    collect_result = []

    for row in dataframe.select("path", metadata_col).toLocalIterator():
        data = row.asDict()
        metadata = json.loads(data[metadata_col])

        for tag in metadata.keys():
            value = metadata[tag].get("value")
            vr = metadata[tag].get("vr")
            collect_result.append([data["path"], tag, vr, value])

    columns = ["Path", "Tag", "VR", "Value"]
    metadata_result_df = pd.DataFrame(collect_result, columns=columns)
    return metadata_result_df

def build_metadata_comparison_df(dataframe, original_col="metadata_original", cleaned_col="metadata_cleaned"):
    import json
    import pandas as pd

    collect_result = []

    for row in dataframe.select("path", original_col, cleaned_col).toLocalIterator():
        data = row.asDict()
        metadata_original = json.loads(data[original_col])
        metadata_cleaned = json.loads(data[cleaned_col])

        for tag in metadata_original.keys():
            original_value = metadata_original[tag]["value"]
            cleaned_value = "DELETED" if tag not in metadata_cleaned else metadata_cleaned[tag]["value"]
            value_changed = False if original_value == cleaned_value else True
            value_deleted = True if original_value != "DELETED" and cleaned_value == "DELETED" else False
            collect_result.append([tag, metadata_original[tag]["vr"], original_value, cleaned_value, value_changed, value_deleted])

    columns = ["Tag", "VR", "Original_Value", "Cleaned_Value", "Is_Changed", "Is_Deleted"]
    metadata_result_df = pd.DataFrame(collect_result, columns=columns)
    return metadata_result_df
```
