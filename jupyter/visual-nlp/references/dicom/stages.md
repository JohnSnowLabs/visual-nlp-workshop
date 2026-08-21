# Stage Catalog

Use this file to understand stage contracts. Do not copy full pipeline code from here; copy pipeline code from the selected route-specific `template_file` in `routing.yaml`.

Shared OCR, text detection, visual coordinate, drawing, and NER stages are intentionally documented in common references:

- OCR recognizers: `common/stages-ocr.md`
- Text detection: `common/stages-text-detection.md`
- Visual coordinate conversion and image/PDF drawing: `common/visual_stage.md`
- NER wiring: `common/stages-ner.md`
- Zero-shot model choices: `common/zero-shot-models.md`

## Input And Keep Rules

- Define the pipeline before runtime data loading. Place `dicom_df = spark.read.format("binaryFile").load(dicom_path)` after the pipeline block and before `pipeline.transform(...)`.
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
  validation: display_dicom(df=result, fields="dicom_pixel_cleaned")

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

Metadata inspection example:

```python
metadata = DicomToMetadata() \
    .setInputCol("content") \
    .setOutputCol("metadata_original") \
    .setKeepInput(False) \
    .setExtractTagForNer(False)
```

Metadata cleanTag extraction example:

```python
metadata_for_ner = DicomToMetadata() \
    .setInputCol("content") \
    .setOutputCol("metadata_original") \
    .setKeepInput(False) \
    .setExtractTagForNer(True) \
    .setTagMappingCol("tag_mapping") \
    .setTagCol("tag_text") \
    .setStrategyFile(strategy_file_path)
```

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

Metadata-only example:

```python
dicom_deidentifier = DicomMetadataDeidentifier() \
    .setInputCols(["path"]) \
    .setOutputCol("dicom_metadata_cleaned") \
    .setKeepInput(True) \
    .setStrategyFile(strategy_file_path) \
    .setRemovePrivateTags(False)
```

After pixel redaction example:

```python
dicom_deidentifier = DicomMetadataDeidentifier() \
    .setInputCols(["dicom_pixel_cleaned"]) \
    .setOutputCol("dicom_metadata_cleaned") \
    .setKeepInput(False) \
    .setStrategyFile(strategy_file_path) \
    .setRemovePrivateTags(False)
```

cleanTag metadata example:

```python
dicom_deidentifier = DicomMetadataDeidentifier() \
    .setInputCols(["path"]) \
    .setOutputCol("dicom_metadata_cleaned") \
    .setTagMappingCol("tag_mapping") \
    .setTagCleanedCol("deid_documents") \
    .setKeepInput(True) \
    .setStrategyFile(strategy_file_path)
```

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
- `setCompressImage(True)`: required only when `MedicalVisionLLM` consumes the image.
- `setCompressImage(False)`: required for `ImageToText`, `ImageToTextV2`, `ImageToTextV3`, and text detection paths.
- `setCompressionMode(...)`: actual compression mode. Values: `disabled`, `enabled`, `auto`.
- `setCompressionQuality(80)`: actual compression quality example.
- `setCompressionThreshold(1)`: `auto` compresses when megapixels are greater than threshold.

VLM image extraction example:

```python
dicom_to_image = DicomToImageV3() \
    .setInputCols(["content"]) \
    .setOutputCol("image") \
    .setCompressionMode(config["compression_mode"]) \
    .setKeepInput(False) \
    .setMemoryOptimized(config["memory_optimized"]) \
    .setCompressionQuality(config["compression_quality"]) \
    .setScale(config["scale"]) \
    .setFrameLimit(config["frame_sampling"]) \
    .setFrameSamplingStrategy(config["frame_sampling_strategy"]) \
    .setCompressImage(True) \
    .setFrameDimsCol("frame_dims")
```

Non-VLM image extraction example:

```python
dicom_to_image = DicomToImageV3() \
    .setInputCols(["content"]) \
    .setOutputCol("image") \
    .setCompressionMode(config["compression_mode"]) \
    .setKeepInput(False) \
    .setMemoryOptimized(config["memory_optimized"]) \
    .setCompressionQuality(config["compression_quality"]) \
    .setScale(config["scale"]) \
    .setFrameLimit(config["frame_sampling"]) \
    .setFrameSamplingStrategy(config["frame_sampling_strategy"]) \
    .setCompressImage(False) \
    .setFrameDimsCol("frame_dims")
```

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

Inspection rule:
- If `DicomDrawRegions` is the last stage, use `display_dicom(df=result, fields="dicom_pixel_cleaned", limit=1, width=300)`.
- Do not select upstream intermediate columns such as `image`, `text`, `regions`, or `coordinates` from a DataFrame after `DicomDrawRegions`.
- If downstream stages run after `DicomDrawRegions`, such as `DicomMetadataDeidentifier` or `DicomToMetadata`, their output columns may be selected because they are produced after the aggregation.
- When the user wants OCR text, regions, or coordinates, create a separate inspection pipeline that stops before `DicomDrawRegions`.

PHI coordinate redaction example:

```python
draw_regions = DicomDrawRegions() \
    .setInputCol("path") \
    .setInputRegionsCol("coordinates") \
    .setOutputCol("dicom_pixel_cleaned") \
    .setAggCols(["path"]) \
    .setKeepInput(True) \
    .setScaleFactor(1 / config["scale"])
```

Visible text-region redaction example:

```python
draw_regions = DicomDrawRegions() \
    .setInputCol("path") \
    .setInputRegionsCol("text_regions") \
    .setOutputCol("dicom_pixel_cleaned") \
    .setAggCols(["path"]) \
    .setKeepInput(True) \
    .setScaleFactor(1 / config["scale"])
```

## Encapsulated PDF Stages

Architecture: `DicomToPdf -> PdfToImage -> OCR/VLM -> build_stacked_zero_shot_ner_pipeline -> PositionFinder -> ImageSchemaConverter -> ImageDrawRegions -> ImageToPdf -> DicomUpdatePdf`.

Contracts:
- Use `DicomToPdf` to extract the encapsulated PDF from DICOM.
- Use `DicomToPdf.setInputCols(["path"]).setKeepInput(True)`; this is grounded in the encapsulated PDF notebooks.
- Use `PdfToImage` to render PDF pages to `image`.
- For encapsulated PDF page rendering, use the same image schema rule as standalone PDF workflows: `setCompressImage(True).setImageDimsCol("frame_dims")` only when `MedicalVisionLLM` consumes page images, and `setCompressImage(False)` for non-VLM OCR or text detection.
- Key `PdfToImage` params for encapsulated PDF routes include `setInputCol("pdf")`, `setOutputCol("image")`, `setResolution(...)`, `setKeepInput(...)`, `setPageNumCol(...)`, `setOriginCol(...)`, `setImageDimsCol(...)`, and `setCompressImage(...)`.
- Use VLM OCR on PDF page images when highest accuracy is requested.
- Use `build_stacked_zero_shot_ner_pipeline(input_text="text", output_chunk="merged_ner_chunk")` for encapsulated PDF PHI detection after OCR produces `text`.
- Convert assembler image schema back to internal before `ImageDrawRegions`.
- Use `ImageDrawRegions` for PDF page redaction, not `DicomDrawRegions`.
- Use `ImageToPdf` to rebuild PDF pages.
- Use `DicomUpdatePdf` to write the cleaned PDF back into DICOM; output is `dicom`.
- Use `DicomUpdatePdf.setInputCol("path").setKeepInput(True)` so `path` is not dropped.

DICOM PDF extraction example:

```python
dicom_to_pdf = DicomToPdf() \
    .setInputCols(["path"]) \
    .setOutputCol("pdf") \
    .setKeepInput(True)
```

DICOM PDF update example:

```python
dicom_update_pdf = DicomUpdatePdf() \
    .setInputCol("path") \
    .setInputPdfCol("pdf_cleaned") \
    .setOutputCol("dicom") \
    .setKeepInput(True)
```
