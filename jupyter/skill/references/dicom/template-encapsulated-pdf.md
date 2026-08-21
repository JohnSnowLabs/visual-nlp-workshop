# Encapsulated PDF Template

## encapsulated_pdf_phi_vlm

Architecture: `DicomToPdf -> PdfToImage -> OCR (VLM/V1/V2/V3) -> build_stacked_zero_shot_ner_pipeline -> PositionFinder -> ImageDrawRegions -> ImageToPdf -> DicomUpdatePdf`.

Run an inspection pass only when the user explicitly asks to view `image`, `text`, `regions`, or `coordinates`. Run the final pass separately because PDF reconstruction may remove or overwrite intermediate columns.

Before this section, include `common_zero_shot_config` and `common_zero_shot_ner_builder` from `common/template-zero-shot-builder.md`, plus `common_zero_shot_position_finder` from `common/visual_stage.md`. Encapsulated PDF pages are dense text, unlike DICOM frames, so override `config["nPredict"]` to a high value (4000+, e.g. `4000`) after loading `common_zero_shot_config` — do not leave it at that shared default of `1024`.

Pick `config["ocr_engine"]` using `models.yaml` `ocr_engine.selection_guide.default_by_hardware` — GPU defaults to VLM, CPU defaults to V1 (V3 or V2 only if requested). Unlike normal DICOM pixel routes, `V1` is valid here. Output only the OCR helper and branch needed by the selected engine: for VLM, include `ocr_vlm_zero_shot_encapsulated_pdf_builder(...)` and do not output the non-VLM helper; for V1/V2/V3, include `ocr_non_vlm_zero_shot_encapsulated_pdf_builder(...)` and do not output the VLM helper.

### DICOM Zero-Shot Encapsulated PDF VLM OCR Helper

```python
def ocr_vlm_zero_shot_encapsulated_pdf_builder(config):

    dicom_to_pdf = DicomToPdf() \
        .setInputCols(["path"]) \
        .setOutputCol("pdf") \
        .setKeepInput(True)

    pdf_to_image = PdfToImage() \
        .setInputCol("pdf") \
        .setOutputCol("image") \
        .setResolution(config["resolution"]) \
        .setCompressImage(True) \
        .setImageDimsCol("frame_dims")

    caption_assembler = DocumentAssembler() \
        .setInputCol("caption") \
        .setOutputCol("caption_document")

    schema_converter_assembler = ImageSchemaConverter() \
        .setInputCol("image") \
        .setOutputCol("image_assembler") \
        .setOutputSchema(ImageSchemaConversion.ASSEMBLER) \
        .setKeepInput(False)

    vlm_ocr = MedicalVisionLLM.pretrained("jsl-ocr-gguf-vlm1", "en", "clinical/ocr") \
        .setInputCols(["caption_document", "image_assembler"]) \
        .setOutputCol("completions") \
        .setNGpuLayers(99) \
        .setNCtx(32768) \
        .setNParallel(1) \
        .setNBatch(2048) \
        .setNUbatch(1024) \
        .setNPredict(config["nPredict"]) \
        .setTemperature(0.01) \
        .setTopK(1) \
        .setTopP(1.0) \
        .setRepeatPenalty(1.03) \
        .setRepeatLastN(256) \
        .setMinKeep(0) \
        .setNProbs(0) \
        .setBatchSize(1) \
        .setDisableLog(False)

    coordinate_extract = DocumentCoordinatesToText() \
        .setInputCol("completions") \
        .setImageDimsCol("frame_dims") \
        .setOutputCol("text") \
        .setPageMatrixCol("positions") \
        .setRegionCol("regions") \
        .setLineTolerance(5)

    return [dicom_to_pdf, pdf_to_image, caption_assembler, schema_converter_assembler, vlm_ocr, coordinate_extract]
```

### DICOM Zero-Shot Encapsulated PDF Non-VLM OCR Helper

```python
def ocr_non_vlm_zero_shot_encapsulated_pdf_builder(config):

    engine = config["ocr_engine"]

    dicom_to_pdf = DicomToPdf() \
        .setInputCols(["path"]) \
        .setOutputCol("pdf") \
        .setKeepInput(True)

    pdf_to_image = PdfToImage() \
        .setInputCol("pdf") \
        .setOutputCol("image") \
        .setResolution(config["resolution"]) \
        .setCompressImage(False) \
        .setImageDimsCol("frame_dims")

    if engine == "v1":
        ocr = ImageToText() \
            .setInputCol("image") \
            .setOutputCol("text") \
            .setPositionsCol("positions") \
            .setIgnoreResolution(False) \
            .setPageIteratorLevel(PageIteratorLevel.SYMBOL) \
            .setPageSegMode(PageSegmentationMode.SPARSE_TEXT) \
            .setWithSpaces(True) \
            .setKeepLayout(False) \
            .setConfidenceThreshold(70)

        return [dicom_to_pdf, pdf_to_image, ocr]

    detector = config["detector_engine"]

    if detector == "v2":
        text_detector = ImageTextDetectorV2.pretrained("image_text_detector_v2", "en", "clinical/ocr") \
            .setInputCol("image") \
            .setOutputCol("text_regions") \
            .setScoreThreshold(0.5) \
            .setTextThreshold(0.2) \
            .setSizeThreshold(10) \
            .setWithRefiner(True) \
            .setUseGPU(config["gpu"])
    else:
        text_detector = ImageTextDetector.pretrained("image_text_detector_mem_opt", "en", "clinical/ocr") \
            .setInputCol("image") \
            .setOutputCol("text_regions") \
            .setScoreThreshold(0.7) \
            .setLinkThreshold(0.5) \
            .setWithRefiner(True) \
            .setTextThreshold(0.4) \
            .setSizeThreshold(-1) \
            .setUseGPU(config["gpu"]) \
            .setWidth(0)

    if engine == "v2":
        ocr = ImageToTextV2.pretrained("ocr_large_printed_v2_opt", "en", "clinical/ocr") \
            .setRegionsColumn("text_regions") \
            .setInputCols(["image"]) \
            .setOutputCol("text") \
            .setOutputFormat(OcrOutputFormat.TEXT_WITH_POSITIONS) \
            .setGroupImages(False) \
            .setKeepInput(True) \
            .setUseGPU(config["gpu"]) \
            .setUseCaching(True)
    elif engine == "v3":
        ocr = ImageToTextV3() \
            .setInputCols(["image", "text_regions"]) \
            .setOutputCol("text")

    return [dicom_to_pdf, pdf_to_image, text_detector, ocr]
```

### DICOM Zero-Shot Encapsulated PDF Config Helpers

```python
def zero_shot_encapsulated_pdf_config_cleaner(config):
    config = dict(config)

    for key in ["ocr_engine", "detector_engine"]:
        value = config.get(key)
        if isinstance(value, str):
            config[key] = value.lower()

    config["is_vlm"] = config.get("ocr_engine") == "vlm"

    return config

def zero_shot_encapsulated_pdf_required_config_keys(config):
    required_keys = ["ocr_engine", "resolution"]

    if config["is_vlm"]:
        required_keys.append("nPredict")
    elif config["ocr_engine"] in ["v2", "v3"]:
        required_keys.extend(["detector_engine", "gpu"])

    return required_keys

def zero_shot_encapsulated_pdf_config_checker(config):
    config = zero_shot_encapsulated_pdf_config_cleaner(config)
    required_keys = zero_shot_encapsulated_pdf_required_config_keys(config)

    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise ValueError(f"Missing DICOM Zero-Shot Encapsulated PDF config key(s): {missing_keys}")

    if config["ocr_engine"] not in ["vlm", "v1", "v2", "v3"]:
        raise ValueError(f"Unsupported DICOM Zero-Shot Encapsulated PDF OCR engine: {config['ocr_engine']!r}. Expected 'VLM', 'V1', 'V2', or 'V3'.")

    if config["ocr_engine"] in ["v2", "v3"] and config["detector_engine"] not in ["v1", "v2"]:
        raise ValueError(f"Unsupported DICOM Zero-Shot Encapsulated PDF detector engine: {config['detector_engine']!r}. Expected 'V1' or 'V2'.")

    return config
```

### DICOM Zero-Shot Encapsulated PDF VLM Usage Example

```python
from pyspark.sql.functions import lit

config["nPredict"] = 4000
config.update({"ocr_engine": "VLM", "resolution": 300})
config = zero_shot_encapsulated_pdf_config_checker(config)

ingestion_stages = ocr_vlm_zero_shot_encapsulated_pdf_builder(config)
phi_detection_model = build_stacked_zero_shot_ner_pipeline(input_text="text", output_chunk="merged_ner_chunk")

schema_converter_internal = ImageSchemaConverter() \
    .setInputCol("image_assembler") \
    .setOutputCol("image") \
    .setOutputSchema(ImageSchemaConversion.INTERNAL) \
    .setKeepInput(False)

image_draw_regions = ImageDrawRegions() \
    .setInputCol("image") \
    .setInputRegionsCol("coordinates") \
    .setOutputCol("image_with_regions")

image_to_pdf = ImageToPdf() \
    .setInputCol("image_with_regions") \
    .setOutputCol("pdf_cleaned")

dicom_update_pdf = DicomUpdatePdf() \
    .setInputCol("path") \
    .setInputPdfCol("pdf_cleaned") \
    .setOutputCol("dicom") \
    .setKeepInput(True)

final_pipeline = PipelineModel(stages=[
    *ingestion_stages,
    phi_detection_model,
    position_finder,
    schema_converter_internal,
    image_draw_regions,
    image_to_pdf,
    dicom_update_pdf
])

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)
vlm_prompt = "Detect and recognize text in the image, and output the text coordinates in a formatted manner."
dicom_with_prompt_df = dicom_df.withColumn("caption", lit(vlm_prompt))

result = final_pipeline.transform(dicom_with_prompt_df).cache()
display_dicom(df=result, fields="dicom", limit=1, width=300)

saved_paths = save_dicom_to_disk(result, dicom_col="dicom", output_dir="/tmp/dicom_deid")
saved_paths[:5]
```

Optional inspection pass:

```python
inspection_pipeline = PipelineModel(stages=[
    *ingestion_stages,
    phi_detection_model,
    position_finder
])

inspection_df = inspection_pipeline.transform(dicom_with_prompt_df).cache()
inspection_df.select("text", "regions", "coordinates").show(10, False)
```

### DICOM Zero-Shot Encapsulated PDF Non-VLM Usage Note

For non-VLM usage (including `V1`), skip `caption`/`nPredict`, build with `ocr_non_vlm_zero_shot_encapsulated_pdf_builder(...)` instead, and skip `schema_converter_internal` since the image never left `INTERNAL` schema:

```python
config.update({"ocr_engine": "V1", "resolution": 300})
config = zero_shot_encapsulated_pdf_config_checker(config)

ingestion_stages = ocr_non_vlm_zero_shot_encapsulated_pdf_builder(config)
phi_detection_model = build_stacked_zero_shot_ner_pipeline(input_text="text", output_chunk="merged_ner_chunk")

image_draw_regions = ImageDrawRegions() \
    .setInputCol("image") \
    .setInputRegionsCol("coordinates") \
    .setOutputCol("image_with_regions")

image_to_pdf = ImageToPdf() \
    .setInputCol("image_with_regions") \
    .setOutputCol("pdf_cleaned")

dicom_update_pdf = DicomUpdatePdf() \
    .setInputCol("path") \
    .setInputPdfCol("pdf_cleaned") \
    .setOutputCol("dicom") \
    .setKeepInput(True)

final_pipeline = PipelineModel(stages=[
    *ingestion_stages,
    phi_detection_model,
    position_finder,
    image_draw_regions,
    image_to_pdf,
    dicom_update_pdf
])

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)
result = final_pipeline.transform(dicom_df).cache()
display_dicom(df=result, fields="dicom", limit=1, width=300)

saved_paths = save_dicom_to_disk(result, dicom_col="dicom", output_dir="/tmp/dicom_deid")
saved_paths[:5]
```

## encapsulated_pdf_phi_builder

Use this builder first for DICOM encapsulated PDF PHI workflows unless the user explicitly asks for zero-shot stacking or a hand-built custom path. This path uses Healthcare NLP `PretrainedPipeline(...)` from `models.yaml` `clinical_pipeline`.

Before this section, include `common_pixel_builder_helpers` from `common/template-pixel-builder-common.md`. Do not show only the `encapsulated_pdf_pipeline_builder(...)` call-site; include all helpers, config, `PretrainedPipeline` loading, and transform call. Include an inspection pass only when the user explicitly asks for intermediate OCR text, regions, coordinates, or page images. Use `dicom_display_utility` from `common/utilities.md` `common_display` and `dicom_save_utility` from `common/utilities.md` `common_save` for final output.

Run an inspection pass only when the user explicitly asks to view `image`, `text`, `regions`, or `coordinates`. Run the final pass separately because PDF reconstruction may remove or overwrite intermediate columns.

When generating code from this template, output only the OCR helper and pipeline-builder branch for the selected OCR engine. For VLM configs, include `ocr_vlm_encapsulated_pdf_builder(...)` and make `encapsulated_pdf_pipeline_builder(...)` call it directly; do not output `ocr_non_vlm_encapsulated_pdf_builder(...)` or an unused non-VLM branch. For non-VLM configs, include only `ocr_non_vlm_encapsulated_pdf_builder(...)` and make `encapsulated_pdf_pipeline_builder(...)` call it directly.

### DICOM Encapsulated PDF VLM OCR Helper

```python
def ocr_vlm_encapsulated_pdf_builder(config):

    dicom_to_pdf = DicomToPdf() \
        .setInputCols(["path"]) \
        .setOutputCol("pdf") \
        .setKeepInput(True)

    pdf_to_image = PdfToImage() \
        .setInputCol("pdf") \
        .setOutputCol("image") \
        .setResolution(config["resolution"]) \
        .setCompressImage(True) \
        .setImageDimsCol("frame_dims")

    caption_assembler = DocumentAssembler() \
        .setInputCol("caption") \
        .setOutputCol("caption_document")

    schema_converter_assembler = ImageSchemaConverter() \
        .setInputCol("image") \
        .setOutputCol("image_assembler") \
        .setOutputSchema(ImageSchemaConversion.ASSEMBLER) \
        .setKeepInput(False)

    vlm_ocr = MedicalVisionLLM.pretrained("jsl-ocr-gguf-vlm1", "en", "clinical/ocr") \
        .setInputCols(["caption_document", "image_assembler"]) \
        .setOutputCol("completions") \
        .setNGpuLayers(99) \
        .setNCtx(32768) \
        .setNParallel(1) \
        .setNBatch(2048) \
        .setNUbatch(1024) \
        .setNPredict(config["nPredict"]) \
        .setTemperature(0.01) \
        .setTopK(1) \
        .setTopP(1.0) \
        .setRepeatPenalty(1.03) \
        .setRepeatLastN(256) \
        .setMinKeep(0) \
        .setNProbs(0) \
        .setBatchSize(1) \
        .setDisableLog(False)

    coordinate_extract = DocumentCoordinatesToText() \
        .setInputCol("completions") \
        .setImageDimsCol("frame_dims") \
        .setOutputCol("text") \
        .setPageMatrixCol("positions") \
        .setRegionCol("regions") \
        .setLineTolerance(config["line_tolerance"]) \
        .setSpaceTolerance(config["space_tolerance"])

    return [dicom_to_pdf, pdf_to_image, caption_assembler, schema_converter_assembler, vlm_ocr, coordinate_extract]
```

### DICOM Encapsulated PDF Non-VLM OCR Helper

```python
def ocr_non_vlm_encapsulated_pdf_builder(config):

    engine = config["ocr_engine"]

    dicom_to_pdf = DicomToPdf() \
        .setInputCols(["path"]) \
        .setOutputCol("pdf") \
        .setKeepInput(True)

    pdf_to_image = PdfToImage() \
        .setInputCol("pdf") \
        .setOutputCol("image") \
        .setResolution(config["resolution"]) \
        .setCompressImage(False) \
        .setImageDimsCol("frame_dims")

    if engine == "v1":
        ocr = ImageToText() \
            .setInputCol("image") \
            .setOutputCol("text") \
            .setPositionsCol("positions") \
            .setIgnoreResolution(False) \
            .setPageIteratorLevel(PageIteratorLevel.SYMBOL) \
            .setPageSegMode(PageSegmentationMode.SPARSE_TEXT) \
            .setWithSpaces(True) \
            .setKeepLayout(False) \
            .setConfidenceThreshold(70)

        return [dicom_to_pdf, pdf_to_image, ocr]

    detector = config["detector_engine"]

    if detector == "v2":
        text_detector = ImageTextDetectorV2.pretrained("image_text_detector_v2", "en", "clinical/ocr") \
            .setInputCol("image") \
            .setOutputCol("text_regions") \
            .setScoreThreshold(0.5) \
            .setTextThreshold(0.2) \
            .setSizeThreshold(10) \
            .setWithRefiner(True) \
            .setUseGPU(config["gpu"])
    else:
        text_detector = ImageTextDetector.pretrained("image_text_detector_mem_opt", "en", "clinical/ocr") \
            .setInputCol("image") \
            .setOutputCol("text_regions") \
            .setScoreThreshold(0.7) \
            .setLinkThreshold(0.5) \
            .setWithRefiner(True) \
            .setTextThreshold(0.4) \
            .setSizeThreshold(-1) \
            .setUseGPU(config["gpu"]) \
            .setWidth(0)

    if engine == "v2":
        ocr = ImageToTextV2.pretrained("ocr_large_printed_v2_opt", "en", "clinical/ocr") \
            .setRegionsColumn("text_regions") \
            .setInputCols(["image"]) \
            .setOutputCol("text") \
            .setOutputFormat(OcrOutputFormat.TEXT_WITH_POSITIONS) \
            .setGroupImages(False) \
            .setKeepInput(True) \
            .setUseGPU(config["gpu"]) \
            .setUseCaching(True)
    elif engine == "v3":
        ocr = ImageToTextV3() \
            .setInputCols(["image", "text_regions"]) \
            .setOutputCol("text")

    return [dicom_to_pdf, pdf_to_image, text_detector, ocr]
```

### DICOM Encapsulated PDF Config Helpers

```python
def encapsulated_pdf_config_cleaner(config):
    config = dict(config)

    for key in ["ocr_engine", "detector_engine"]:
        value = config.get(key)
        if isinstance(value, str):
            config[key] = value.lower()

    config["is_vlm"] = config.get("ocr_engine") == "vlm"

    return config

def encapsulated_pdf_required_config_keys(config):
    required_keys = ["ocr_engine", "resolution"]

    if config["is_vlm"]:
        required_keys.extend(["nPredict", "line_tolerance", "space_tolerance"])
    elif config["ocr_engine"] in ["v2", "v3"]:
        required_keys.extend(["detector_engine", "gpu"])

    return required_keys

def encapsulated_pdf_config_checker(config):
    config = encapsulated_pdf_config_cleaner(config)
    required_keys = encapsulated_pdf_required_config_keys(config)

    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise ValueError(f"Missing DICOM Encapsulated PDF Pixel Builder config key(s): {missing_keys}")

    if config["ocr_engine"] not in ["vlm", "v1", "v2", "v3"]:
        raise ValueError(f"Unsupported DICOM Encapsulated PDF OCR engine: {config['ocr_engine']!r}. Expected 'VLM', 'V1', 'V2', or 'V3'.")

    if config["ocr_engine"] in ["v2", "v3"] and config["detector_engine"] not in ["v1", "v2"]:
        raise ValueError(f"Unsupported DICOM Encapsulated PDF detector engine: {config['detector_engine']!r}. Expected 'V1' or 'V2'.")

    return config

```

### DICOM Encapsulated PDF VLM Pipeline Builder

Use this builder only when `config["ocr_engine"]` is `VLM`. Do not include the non-VLM helper or branch in the generated code.

```python
def encapsulated_pdf_pipeline_builder(spark_session, config, intermediate_result, pretrained_pipeline):
    config = encapsulated_pdf_config_checker(config)
    ingestion_stages = ocr_vlm_encapsulated_pdf_builder(config)

    ner_output, nlp_pipeline = nlp_builder(spark_session, pretrained_pipeline)

    position_finder = PositionFinder() \
        .setInputCols([ner_output]) \
        .setOutputCol("coordinates") \
        .setPageMatrixCol("positions")

    final_stages = [*ingestion_stages, nlp_pipeline, position_finder]

    if intermediate_result:
        return PipelineModel(stages=final_stages)

    schema_converter_internal = ImageSchemaConverter() \
        .setInputCol("image_assembler") \
        .setOutputCol("image") \
        .setOutputSchema(ImageSchemaConversion.INTERNAL) \
        .setKeepInput(False)

    image_draw_regions = ImageDrawRegions() \
        .setInputCol("image") \
        .setInputRegionsCol("coordinates") \
        .setOutputCol("image_with_regions")

    image_to_pdf = ImageToPdf() \
        .setInputCol("image_with_regions") \
        .setOutputCol("pdf_cleaned")

    dicom_update_pdf = DicomUpdatePdf() \
        .setInputCol("path") \
        .setInputPdfCol("pdf_cleaned") \
        .setOutputCol("dicom") \
        .setKeepInput(True)

    final_stages.extend([schema_converter_internal, image_draw_regions, image_to_pdf, dicom_update_pdf])

    return PipelineModel(stages=final_stages)
```

### DICOM Encapsulated PDF Non-VLM Pipeline Builder

Use this builder only when `config["ocr_engine"]` is `V1`, `V2`, or `V3`. Do not include the VLM helper or branch in the generated code.

```python
def encapsulated_pdf_pipeline_builder(spark_session, config, intermediate_result, pretrained_pipeline):
    config = encapsulated_pdf_config_checker(config)
    ingestion_stages = ocr_non_vlm_encapsulated_pdf_builder(config)

    ner_output, nlp_pipeline = nlp_builder(spark_session, pretrained_pipeline)

    position_finder = PositionFinder() \
        .setInputCols([ner_output]) \
        .setOutputCol("coordinates") \
        .setPageMatrixCol("positions")

    final_stages = [*ingestion_stages, nlp_pipeline, position_finder]

    if intermediate_result:
        return PipelineModel(stages=final_stages)

    image_draw_regions = ImageDrawRegions() \
        .setInputCol("image") \
        .setInputRegionsCol("coordinates") \
        .setOutputCol("image_with_regions")

    image_to_pdf = ImageToPdf() \
        .setInputCol("image_with_regions") \
        .setOutputCol("pdf_cleaned")

    dicom_update_pdf = DicomUpdatePdf() \
        .setInputCol("path") \
        .setInputPdfCol("pdf_cleaned") \
        .setOutputCol("dicom") \
        .setKeepInput(True)

    final_stages.extend([image_draw_regions, image_to_pdf, dicom_update_pdf])

    return PipelineModel(stages=final_stages)
```

### DICOM Encapsulated PDF Config Examples

Use one of these config blocks. VLM configs require a `caption` column. Non-VLM configs do not use `caption`, `nPredict`, `line_tolerance`, or `space_tolerance`. `V1` is a valid engine here — unlike normal DICOM pixel routes, DICOM encapsulated PDF may suggest `ImageToText` (V1); `V1` configs skip detection entirely and do not use `detector_engine` or `gpu`.

DICOM Encapsulated PDF VLM config:

```python
config = {
    "resolution": 300,
    "ocr_engine": "VLM",
    "nPredict": 4000,
    "line_tolerance": 5,
    "space_tolerance": 15,
}
```

DICOM Encapsulated PDF V1 config:

```python
config = {
    "resolution": 300,
    "ocr_engine": "V1",
}
```

DICOM Encapsulated PDF detected non-VLM config:

```python
config = {
    "resolution": 300,
    "gpu": True,
    "ocr_engine": "V2",
    "detector_engine": "V1",
}
```

### DICOM Encapsulated PDF VLM Usage Example

```python
from sparknlp.pretrained import PretrainedPipeline
from pyspark.sql.functions import lit

clinical_pipeline_name = "clinical_deidentification_docwise_benchmark_medium"

config = {
    "resolution": 300,
    "ocr_engine": "VLM",
    "nPredict": 4000,
    "line_tolerance": 5,
    "space_tolerance": 15,
}

deid_pipeline = PretrainedPipeline(clinical_pipeline_name, "en", "clinical/models")

pipeline = encapsulated_pdf_pipeline_builder(
    spark_session=spark,
    config=config,
    intermediate_result=False,
    pretrained_pipeline=deid_pipeline,
)

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)
vlm_prompt = "Detect and recognize text in the image, and output the text coordinates in a formatted manner."
dicom_with_prompt_df = dicom_df.withColumn("caption", lit(vlm_prompt))

result = pipeline.transform(dicom_with_prompt_df).cache()
display_dicom(df=result, fields="dicom", limit=1, width=300)
saved_paths = save_dicom_to_disk(result, dicom_col="dicom", output_dir="/tmp/dicom_deid")
saved_paths[:5]
```

### DICOM Encapsulated PDF Non-VLM Usage Note

For non-VLM usage, use the non-VLM config above, skip the `caption` column, and transform `dicom_df` directly:

```python
pipeline = encapsulated_pdf_pipeline_builder(
    spark_session=spark,
    config=config,
    intermediate_result=False,
    pretrained_pipeline=deid_pipeline,
)

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)
result = pipeline.transform(dicom_df).cache()
```

### Optional DICOM Encapsulated PDF Inspection Pass

Include this block only when the user explicitly asks for intermediate results. This inspection pipeline stops before PDF reconstruction, so selecting `text` and `coordinates` is valid here.

```python
intermediate_pipeline = encapsulated_pdf_pipeline_builder(
    spark_session=spark,
    config=config,
    intermediate_result=True,
    pretrained_pipeline=deid_pipeline,
)

intermediate_df = intermediate_pipeline.transform(dicom_with_prompt_df).cache()
intermediate_df.select("text", "coordinates").show(10, False)
```
