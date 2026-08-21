# DICOM Pixel Builder Template

Use this builder first for DICOM pixel PHI workflows unless the user explicitly asks for zero-shot stacking, blanket text removal, or a hand-built custom path. This path uses Healthcare NLP `PretrainedPipeline(...)` from `models.yaml` `clinical_pipeline`. It performs pixel redaction by default; set `config["apply_metadata_deid"] = True` only when the user also wants metadata de-identification.

Before this section, include `common_pixel_builder_helpers` from `common/template-pixel-builder-common.md`. Do not show only the `dicom_pipeline_builder(...)` call-site; include all DICOM helpers, config, `PretrainedPipeline` loading, and transform call. Include an inspection pass only when the user explicitly asks for intermediate OCR text, regions, coordinates, or page images. Use `dicom_display_utility` from `common/utilities.md` `common_display` and `dicom_save_utility` from `common/utilities.md` `common_save` for final output.

When generating code from this template, output only the OCR helper and pipeline-builder branch for the selected OCR engine. For VLM configs, include `ocr_vlm_pipeline_builder(...)` and make `dicom_pipeline_builder(...)` call it directly; do not output `ocr_non_vlm_pipeline_builder(...)` or an unused non-VLM branch. For non-VLM configs, include only `ocr_non_vlm_pipeline_builder(...)` and make `dicom_pipeline_builder(...)` call it directly.

## dicom_pixel_builder

### DICOM VLM OCR Helper

```python
def ocr_vlm_pipeline_builder(config):

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

    return [dicom_to_image, caption_assembler, schema_converter_assembler, vlm_ocr, coordinate_extract]
```

### DICOM Non-VLM OCR Helper

```python
def ocr_non_vlm_pipeline_builder(config):

    engine = config["ocr_engine"]
    detector = config["detector_engine"]

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

    return [dicom_to_image, text_detector, ocr]
```

### DICOM Config Helpers

```python
def dicom_config_cleaner(config):
    config = dict(config)

    for key in ["ocr_engine", "compression_mode", "detector_engine"]:
        value = config.get(key)
        if isinstance(value, str):
            config[key] = value.lower()

    apply_metadata_deid = config.get("apply_metadata_deid", False)

    if isinstance(apply_metadata_deid, str):
        apply_metadata_deid = apply_metadata_deid.lower() == "true"

    config["apply_metadata_deid"] = apply_metadata_deid
    config["is_vlm"] = config.get("ocr_engine") == "vlm"

    return config


def dicom_required_config_keys(config):
    required_keys = [
        "ocr_engine",
        "scale",
        "frame_sampling",
        "frame_sampling_strategy",
        "compression_quality",
        "compression_mode",
        "memory_optimized",
    ]

    if config["is_vlm"]:
        required_keys.extend(["nPredict", "line_tolerance", "space_tolerance"])
    else:
        required_keys.extend(["detector_engine", "gpu"])

    if config["apply_metadata_deid"]:
        required_keys.extend(["strategy_file_path", "remove_private_tags"])

    return required_keys


def dicom_config_checker(config):
    config = dicom_config_cleaner(config)
    required_keys = dicom_required_config_keys(config)

    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise ValueError(f"Missing DICOM Pixel Builder config key(s): {missing_keys}")

    if config["ocr_engine"] not in ["vlm", "v2", "v3"]:
        raise ValueError(f"Unsupported DICOM OCR engine: {config['ocr_engine']!r}. Expected 'VLM', 'V2', or 'V3'.")

    if not config["is_vlm"] and config["detector_engine"] not in ["v1", "v2"]:
        raise ValueError(f"Unsupported DICOM detector engine: {config['detector_engine']!r}. Expected 'V1' or 'V2'.")

    if config["apply_metadata_deid"] and not config["strategy_file_path"]:
        raise ValueError("DICOM Pixel Builder requires config['strategy_file_path'].")

    return config


```

### DICOM VLM Pipeline Builder

Use this builder only when `config["ocr_engine"]` is `VLM`. Do not include the non-VLM helper or branch in the generated code.

```python
def dicom_pipeline_builder(spark_session, config, intermediate_result, pretrained_pipeline, dicom_metadata_deid=None):
    config = dicom_config_checker(config)
    ingestion_stages = ocr_vlm_pipeline_builder(config)

    ner_output, nlp_pipeline = nlp_builder(spark_session, pretrained_pipeline)

    position_finder = PositionFinder() \
        .setInputCols([ner_output]) \
        .setOutputCol("coordinates") \
        .setPageMatrixCol("positions") \
        .setIgnoreSchema(False) \
        .setOcrScaleFactor(1.1)

    final_stages = [*ingestion_stages, nlp_pipeline, position_finder]

    if intermediate_result:
        return PipelineModel(stages=final_stages)

    draw_regions = DicomDrawRegions() \
        .setInputCol("path") \
        .setInputRegionsCol("coordinates") \
        .setOutputCol("dicom_pixel_cleaned") \
        .setAggCols(["path"]) \
        .setKeepInput(True) \
        .setScaleFactor(1 / config["scale"])

    final_stages.extend([draw_regions])

    if not config["apply_metadata_deid"]:
        return PipelineModel(stages=final_stages)

    if dicom_metadata_deid is None:
        dicom_deidentifier = DicomMetadataDeidentifier() \
            .setInputCols(["dicom_pixel_cleaned"]) \
            .setOutputCol("dicom_metadata_cleaned") \
            .setKeepInput(False) \
            .setStrategyFile(config["strategy_file_path"]) \
            .setRemovePrivateTags(config["remove_private_tags"])
    else:
        dicom_deidentifier = dicom_metadata_deid

    original_metadata = DicomToMetadata() \
        .setInputCol("path") \
        .setOutputCol("metadata_original") \
        .setKeepInput(True) \
        .setExtractTagForNer(False)

    final_metadata = DicomToMetadata() \
        .setInputCol("dicom_metadata_cleaned") \
        .setOutputCol("metadata_cleaned") \
        .setKeepInput(True) \
        .setExtractTagForNer(False)

    final_stages.extend([dicom_deidentifier, original_metadata, final_metadata])

    return PipelineModel(stages=final_stages)
```

### DICOM Non-VLM Pipeline Builder

Use this builder only when `config["ocr_engine"]` is `V2` or `V3`. Do not include the VLM helper or branch in the generated code.

```python
def dicom_pipeline_builder(spark_session, config, intermediate_result, pretrained_pipeline, dicom_metadata_deid=None):
    config = dicom_config_checker(config)
    ingestion_stages = ocr_non_vlm_pipeline_builder(config)

    ner_output, nlp_pipeline = nlp_builder(spark_session, pretrained_pipeline)

    position_finder = PositionFinder() \
        .setInputCols([ner_output]) \
        .setOutputCol("coordinates") \
        .setPageMatrixCol("positions") \
        .setIgnoreSchema(False) \
        .setOcrScaleFactor(1.1)

    final_stages = [*ingestion_stages, nlp_pipeline, position_finder]

    if intermediate_result:
        return PipelineModel(stages=final_stages)

    draw_regions = DicomDrawRegions() \
        .setInputCol("path") \
        .setInputRegionsCol("coordinates") \
        .setOutputCol("dicom_pixel_cleaned") \
        .setAggCols(["path"]) \
        .setKeepInput(True) \
        .setScaleFactor(1 / config["scale"])

    final_stages.extend([draw_regions])

    if not config["apply_metadata_deid"]:
        return PipelineModel(stages=final_stages)

    if dicom_metadata_deid is None:
        dicom_deidentifier = DicomMetadataDeidentifier() \
            .setInputCols(["dicom_pixel_cleaned"]) \
            .setOutputCol("dicom_metadata_cleaned") \
            .setKeepInput(False) \
            .setStrategyFile(config["strategy_file_path"]) \
            .setRemovePrivateTags(config["remove_private_tags"])
    else:
        dicom_deidentifier = dicom_metadata_deid

    original_metadata = DicomToMetadata() \
        .setInputCol("path") \
        .setOutputCol("metadata_original") \
        .setKeepInput(True) \
        .setExtractTagForNer(False)

    final_metadata = DicomToMetadata() \
        .setInputCol("dicom_metadata_cleaned") \
        .setOutputCol("metadata_cleaned") \
        .setKeepInput(True) \
        .setExtractTagForNer(False)

    final_stages.extend([dicom_deidentifier, original_metadata, final_metadata])

    return PipelineModel(stages=final_stages)
```

### DICOM Config Examples

Use one of these config blocks. VLM configs require a `caption` column. Non-VLM configs do not use `caption`, `nPredict`, `line_tolerance`, or `space_tolerance`. Metadata de-identification is off by default; turn it on only when the user asks to clean DICOM metadata along with pixels.

DICOM VLM config:

```python
config = {
    "apply_metadata_deid": False,
    "scale": 0.75,
    "frame_sampling": 5,
    "frame_sampling_strategy": FrameSamplingStrategy.CONSECUTIVE,
    "compression_quality": 80,
    "compression_mode": "disabled",
    "memory_optimized": False,
    "ocr_engine": "VLM",
    "nPredict": 1024,
    "line_tolerance": 5,
    "space_tolerance": 15,
}
```

DICOM non-VLM config:

```python
config = {
    "apply_metadata_deid": False,
    "scale": 0.75,
    "frame_sampling": 5,
    "frame_sampling_strategy": FrameSamplingStrategy.CONSECUTIVE,
    "compression_quality": 80,
    "compression_mode": "disabled",
    "memory_optimized": False,
    "gpu": True,
    "ocr_engine": "V2",
    "detector_engine": "V1",
}
```

Optional metadata de-identification settings:

```python
config.update({
    "apply_metadata_deid": True,
    "strategy_file_path": strategy_file_path,
    "remove_private_tags": False,
})
```

### DICOM VLM Usage Example

```python
from sparknlp.pretrained import PretrainedPipeline
from pyspark.sql.functions import lit

clinical_pipeline_name = "clinical_deidentification_docwise_benchmark_medium"

config = {
    "apply_metadata_deid": False,
    "scale": 0.75,
    "frame_sampling": 5,
    "frame_sampling_strategy": FrameSamplingStrategy.CONSECUTIVE,
    "compression_quality": 80,
    "compression_mode": "disabled",
    "memory_optimized": False,
    "ocr_engine": "VLM",
    "nPredict": 1024,
    "line_tolerance": 5,
    "space_tolerance": 15,
}

deid_pipeline = PretrainedPipeline(clinical_pipeline_name, "en", "clinical/models")

pipeline = dicom_pipeline_builder(
    spark_session=spark,
    config=config,
    intermediate_result=False,
    pretrained_pipeline=deid_pipeline,
    dicom_metadata_deid=None,
)

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)
vlm_prompt = "Detect and recognize text in the image, and output the text coordinates in a formatted manner."
dicom_with_prompt_df = dicom_df.withColumn("caption", lit(vlm_prompt))

result = pipeline.transform(dicom_with_prompt_df).cache()
display_dicom(df=result, fields="dicom_pixel_cleaned", limit=1, width=300)
saved_paths = save_dicom_to_disk(result, dicom_col="dicom_pixel_cleaned", output_dir="/tmp/dicom_deid")
saved_paths[:5]
```

### DICOM Pixel Plus Metadata Usage Note

When `config["apply_metadata_deid"]` is `True`, create the strategy CSV, write it to disk, and set `strategy_file_path` plus `remove_private_tags` in `config` before building the pipeline. Use `dicom_metadata_cleaned` as the final DICOM column.

```python
config.update(
    {
        "apply_metadata_deid": True,
        "strategy_file_path": strategy_file_path,
        "remove_private_tags": False,
    }
)

pipeline = dicom_pipeline_builder(
    spark_session=spark,
    config=config,
    intermediate_result=False,
    pretrained_pipeline=deid_pipeline,
    dicom_metadata_deid=None,
)

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)
vlm_prompt = "Detect and recognize text in the image, and output the text coordinates in a formatted manner."
dicom_with_prompt_df = dicom_df.withColumn("caption", lit(vlm_prompt))
result = pipeline.transform(dicom_with_prompt_df).cache()
metadata_result_df = build_metadata_comparison_df(result)
display_dicom(df=result, fields="dicom_metadata_cleaned", limit=1, width=300)
saved_paths = save_dicom_to_disk(result, dicom_col="dicom_metadata_cleaned", output_dir="/tmp/dicom_deid")
saved_paths[:5]
```

### Optional DICOM Inspection Pass

Include this block only when the user explicitly asks for intermediate results. This pipeline stops before `DicomDrawRegions`, so selecting `text` and `coordinates` is valid here. Do not run this selection on the final pipeline result after `DicomDrawRegions`.

```python
intermediate_pipeline = dicom_pipeline_builder(
    spark_session=spark,
    config=config,
    intermediate_result=True,
    pretrained_pipeline=deid_pipeline,
    dicom_metadata_deid=None,
)

intermediate_df = intermediate_pipeline.transform(dicom_with_prompt_df).cache()
intermediate_df.select("text", "coordinates").show(10, False)
```

### DICOM Non-VLM Usage Note

For non-VLM usage, use the DICOM non-VLM config above, skip the `caption` column, and transform `dicom_df` directly:

```python
pipeline = dicom_pipeline_builder(
    spark_session=spark,
    config=config,
    intermediate_result=False,
    pretrained_pipeline=deid_pipeline,
    dicom_metadata_deid=None,
)

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)
result = pipeline.transform(dicom_df).cache()
```
