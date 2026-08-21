# Zero-Shot Templates

Use this file only when routing selects `metadata_clean_tag_ner` or `pixel_phi_zero_shot`, or when the user explicitly asks for zero-shot, stacked models, or configurable entity coverage.

Read `models.yaml` `zero_shot_ner_models.options` for model names, model-card links, and entity lists. Do not duplicate the model catalog here.

## Shared Config Rule

Before generating user code, start with this copy-ready default `config` from `models.yaml`. The labels below are only a starter example for the default model; the full zero-shot model catalog, links, and entity lists live in `models.yaml`. Add more model entries only when the user asks to stack more models or customize entity coverage.

```python
subentity_merged_medium_labels = [
    "DOCTOR",
    "PATIENT",
    "AGE",
    "DATE",
    "HOSPITAL",
    "CITY",
    "STREET",
    "STATE",
    "COUNTRY",
    "PHONE",
    "IDNUM",
    "EMAIL",
    "ZIP",
    "ORGANIZATION",
    "PROFESSION",
    "USERNAME",
]

zero_shot_models = [
    {
        "name": "zeroshot_ner_deid_subentity_merged_medium",
        "labels": subentity_merged_medium_labels,
        "output_col": "subentity_merged_medium_ner",
        "chunk_col": "subentity_merged_medium_chunk",
    }
]

config = {
    "zero_shot_models": zero_shot_models,
    "nPredict": 1024,
    "scale": 0.75,
    "frame_sampling": 5,
    "frame_sampling_strategy": FrameSamplingStrategy.CONSECUTIVE,
}
```

## metadata_clean_tag_ner

Use `build_stacked_zero_shot_metadata_pipeline(...)` for this workflow. Do not use `build_stacked_zero_shot_ner_pipeline(...)` for metadata cleanTag.

```python
from textwrap import dedent

strategy_file_path = "dicom_metadata_clean_tag_strategy.csv"

csv_clean_tag_data = dedent("""\
Tags,VR,Name,Status,Action,Option
"(0008, 1030)",LO,Study Description,,cleanTag,deid
"(0018, 1030)",LO,Protocol Name,,cleanTag,deid
"(0040, 4000)",LT,Comments on the Performed Procedure Step,,cleanTag,deid
""")

with open(strategy_file_path, "w", encoding="utf-8", newline="") as file:
    file.write(csv_clean_tag_data)

print(f"Strategy file saved to: {strategy_file_path}")

def build_stacked_zero_shot_metadata_pipeline(input_text="tag_text"):
    document_assembler = DocumentAssembler() \
        .setInputCol(input_text) \
        .setOutputCol("t_document")

    sentence_detector = SentenceDetector() \
        .setInputCols(["t_document"]) \
        .setOutputCol("t_sentence") \
        .setCustomBounds(["<dicom>"]) \
        .setUseCustomBoundsOnly(True)

    tokenizer = Tokenizer() \
        .setInputCols(["t_sentence"]) \
        .setOutputCol("t_token")

    stages = [document_assembler, sentence_detector, tokenizer]
    chunk_cols = []

    for model_settings in config["zero_shot_models"]:
        zero_shot_ner = PretrainedZeroShotNER().pretrained(model_settings["name"], "en", "clinical/models") \
            .setInputCols(["t_sentence", "t_token"]) \
            .setOutputCol(model_settings["output_col"]) \
            .setPredictionThreshold(0.5) \
            .setLabels(model_settings["labels"])

        ner_converter = NerConverterInternal() \
            .setInputCols(["t_sentence", "t_token", model_settings["output_col"]]) \
            .setOutputCol(model_settings["chunk_col"])

        stages.extend([zero_shot_ner, ner_converter])
        chunk_cols.append(model_settings["chunk_col"])

    chunk_merger = ChunkMergeApproach() \
        .setInputCols(chunk_cols) \
        .setOutputCol("t_ner_chunk")

    deid_documents = DeIdentification() \
        .setInputCols(["t_sentence", "t_token", "t_ner_chunk"]) \
        .setOutputCol("deid_documents") \
        .setMode("deid")

    stages.extend([chunk_merger, deid_documents])

    nlp_pipeline = Pipeline(stages=stages)
    empty_data = spark.createDataFrame([[""]], [input_text])
    return nlp_pipeline.fit(empty_data)

dicom_to_metadata = DicomToMetadata() \
    .setInputCol("content") \
    .setOutputCol("metadata_original") \
    .setKeepInput(False) \
    .setExtractTagForNer(True) \
    .setTagMappingCol("tag_mapping") \
    .setTagCol("tag_text") \
    .setStrategyFile(strategy_file_path)

dicom_deidentifier = DicomMetadataDeidentifier() \
    .setInputCols(["path"]) \
    .setOutputCol("dicom_metadata_cleaned") \
    .setTagMappingCol("tag_mapping") \
    .setTagCleanedCol("deid_documents") \
    .setKeepInput(True) \
    .setStrategyFile(strategy_file_path)

deid_metadata = DicomToMetadata() \
    .setInputCol("dicom_metadata_cleaned") \
    .setOutputCol("metadata_cleaned") \
    .setKeepInput(True) \
    .setExtractTagForNer(False)

metadata_pipeline = build_stacked_zero_shot_metadata_pipeline(input_text="tag_text")

pipeline = PipelineModel(stages=[
    dicom_to_metadata,
    metadata_pipeline,
    dicom_deidentifier,
    deid_metadata,
])

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)
result = pipeline.transform(dicom_df).cache()
display_dicom(df=result, fields="dicom_metadata_cleaned", limit=1, width=300)

metadata_result_df = build_metadata_comparison_df(result)
metadata_result_df.head()

saved_paths = save_dicom_to_disk(result, dicom_col="dicom_metadata_cleaned", output_dir="/tmp/dicom_deid")
saved_paths[:5]
```

## pixel_phi_zero_shot

This template is the zero-shot alternative to the builder. Do not call `dicom_pipeline_builder(...)` here. Use `build_stacked_zero_shot_ner_pipeline(...)` only for pixel de-identification without the pipeline builder.

`DicomDrawRegions` is an aggregation stage. Use a separate inspection pipeline that stops before `DicomDrawRegions` if the user wants `text`, `regions`, or `coordinates`; after this final stage, validate with `display_dicom(...)` on `dicom_pixel_cleaned`.

Before this section, include `common_zero_shot_config` and `common_zero_shot_ner_builder` from `common/template-zero-shot-builder.md`, plus `common_zero_shot_position_finder` from `common/visual_stage.md`.

Pick `config["ocr_engine"]` using `models.yaml` `ocr_engine.selection_guide.default_by_hardware` — GPU defaults to VLM, CPU defaults to V3 (never V1 for this route). Output only the OCR helper and branch needed by the selected engine: for VLM, include `ocr_vlm_zero_shot_pixel_builder(...)` and do not output the non-VLM helper; for V2/V3, include `ocr_non_vlm_zero_shot_pixel_builder(...)` and do not output the VLM helper.

### DICOM Zero-Shot Pixel VLM OCR Helper

```python
def ocr_vlm_zero_shot_pixel_builder(config):

    dicom_to_image = DicomToImageV3() \
        .setInputCols(["content"]) \
        .setOutputCol("image") \
        .setKeepInput(False) \
        .setScale(config["scale"]) \
        .setFrameLimit(config["frame_sampling"]) \
        .setFrameSamplingStrategy(config["frame_sampling_strategy"]) \
        .setCompressImage(True) \
        .setFrameDimsCol("frame_dims")

    caption_assembler = DocumentAssembler() \
        .setInputCol("caption") \
        .setOutputCol("caption_document")

    image_assembler = ImageSchemaConverter() \
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

    return [dicom_to_image, caption_assembler, image_assembler, vlm_ocr, coordinate_extract]
```

### DICOM Zero-Shot Pixel Non-VLM OCR Helper

Use this only when `config["ocr_engine"]` is `V2` or `V3`. `V1` is never valid for this route — use `V3` for the cheapest CPU-only path instead.

```python
def ocr_non_vlm_zero_shot_pixel_builder(config):

    engine = config["ocr_engine"]

    dicom_to_image = DicomToImageV3() \
        .setInputCols(["content"]) \
        .setOutputCol("image") \
        .setKeepInput(False) \
        .setScale(config["scale"]) \
        .setFrameLimit(config["frame_sampling"]) \
        .setFrameSamplingStrategy(config["frame_sampling_strategy"]) \
        .setCompressImage(False) \
        .setFrameDimsCol("frame_dims")

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

    return [dicom_to_image, text_detector, ocr]
```

### DICOM Zero-Shot Pixel Config Helpers

```python
def zero_shot_pixel_config_cleaner(config):
    config = dict(config)

    for key in ["ocr_engine", "detector_engine"]:
        value = config.get(key)
        if isinstance(value, str):
            config[key] = value.lower()

    config["is_vlm"] = config.get("ocr_engine") == "vlm"

    return config

def zero_shot_pixel_required_config_keys(config):
    required_keys = ["ocr_engine", "scale", "frame_sampling", "frame_sampling_strategy"]

    if config["is_vlm"]:
        required_keys.append("nPredict")
    elif config["ocr_engine"] in ["v2", "v3"]:
        required_keys.extend(["detector_engine", "gpu"])

    return required_keys

def zero_shot_pixel_config_checker(config):
    config = zero_shot_pixel_config_cleaner(config)
    required_keys = zero_shot_pixel_required_config_keys(config)

    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise ValueError(f"Missing DICOM Zero-Shot Pixel config key(s): {missing_keys}")

    if config["ocr_engine"] not in ["vlm", "v2", "v3"]:
        raise ValueError(f"Unsupported DICOM Zero-Shot Pixel OCR engine: {config['ocr_engine']!r}. Expected 'VLM', 'V2', or 'V3'.")

    if config["ocr_engine"] in ["v2", "v3"] and config["detector_engine"] not in ["v1", "v2"]:
        raise ValueError(f"Unsupported DICOM Zero-Shot Pixel detector engine: {config['detector_engine']!r}. Expected 'V1' or 'V2'.")

    return config
```

### DICOM Zero-Shot Pixel Config Examples

VLM config:

```python
config.update({
    "ocr_engine": "VLM",
    "nPredict": 1024,
    "scale": 0.75,
    "frame_sampling": 5,
    "frame_sampling_strategy": FrameSamplingStrategy.CONSECUTIVE,
})
```

Non-VLM config:

```python
config.update({
    "ocr_engine": "V3",
    "detector_engine": "V1",
    "gpu": True,
    "scale": 0.75,
    "frame_sampling": 5,
    "frame_sampling_strategy": FrameSamplingStrategy.CONSECUTIVE,
})
```

### DICOM Zero-Shot Pixel VLM Usage Example

```python
from pyspark.sql.functions import lit

config = zero_shot_pixel_config_checker(config)
ingestion_stages = ocr_vlm_zero_shot_pixel_builder(config)

position_finder = PositionFinder() \
    .setInputCols(["merged_ner_chunk"]) \
    .setOutputCol("coordinates") \
    .setPageMatrixCol("positions") \
    .setIgnoreSchema(False) \
    .setOcrScaleFactor(1.1)

draw_regions = DicomDrawRegions() \
    .setInputCol("path") \
    .setInputRegionsCol("coordinates") \
    .setOutputCol("dicom_pixel_cleaned") \
    .setAggCols(["path"]) \
    .setKeepInput(True) \
    .setScaleFactor(1 / config["scale"])

pipeline = PipelineModel(stages=[
    *ingestion_stages,
    build_stacked_zero_shot_ner_pipeline(input_text="text", output_chunk="merged_ner_chunk"),
    position_finder,
    draw_regions
])

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)
vlm_prompt = "Detect and recognize text in the image, and output the text coordinates in a formatted manner."
dicom_with_prompt_df = dicom_df.withColumn("caption", lit(vlm_prompt))

result = pipeline.transform(dicom_with_prompt_df).cache()
display_dicom(df=result, fields="dicom_pixel_cleaned", limit=1, width=300)

saved_paths = save_dicom_to_disk(result, dicom_col="dicom_pixel_cleaned", output_dir="/tmp/dicom_deid")
saved_paths[:5]
```

### DICOM Zero-Shot Pixel Non-VLM Usage Note

For non-VLM usage, use the non-VLM config above, build with `ocr_non_vlm_zero_shot_pixel_builder(...)` instead, skip the `caption` column, and transform `dicom_df` directly:

```python
config = zero_shot_pixel_config_checker(config)
ingestion_stages = ocr_non_vlm_zero_shot_pixel_builder(config)

pipeline = PipelineModel(stages=[
    *ingestion_stages,
    build_stacked_zero_shot_ner_pipeline(input_text="text", output_chunk="merged_ner_chunk"),
    position_finder,
    draw_regions
])

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)
result = pipeline.transform(dicom_df).cache()
```
