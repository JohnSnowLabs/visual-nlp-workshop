# Image/PDF Pixel Builder Template

Use this section for shared standalone image and PDF Pixel Builder code.

Before this section, include `common_pixel_builder_helpers` from `common/template-pixel-builder-common.md`. The ingestion helpers below handle both input types through `config["input_type"]`. Use the image or PDF display/save utility sections from `common/utilities.md` `common_display` and `common_save` for final output.

When generating code from this template, output only the OCR helper and pipeline-builder branch for the selected OCR engine. For VLM configs, include `ocr_vlm_image_pdf_ingestion_builder(...)` and make `image_pdf_pipeline_builder(...)` call it directly; do not output `ocr_non_vlm_image_pdf_ingestion_builder(...)` or an unused non-VLM branch. For non-VLM configs, include only `ocr_non_vlm_image_pdf_ingestion_builder(...)` and make `image_pdf_pipeline_builder(...)` call it directly.

## image_pdf_pixel_builder

### Image/PDF VLM OCR Helper

```python
def ocr_vlm_image_pdf_ingestion_builder(config):

    input_type = config["input_type"]

    if input_type == "pdf":
        ingestion = PdfToImage() \
            .setInputCol("content") \
            .setOutputCol("image") \
            .setKeepInput(False) \
            .setCompressImage(True) \
            .setResolution(config["resolution"]) \
            .setImageDimsCol("frame_dims")
    else:
        ingestion = BinaryToImage() \
            .setInputCol("content") \
            .setOutputCol("image") \
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

    return [ingestion, caption_assembler, schema_converter_assembler, vlm_ocr, coordinate_extract]
```

### Image/PDF Non-VLM OCR Helper

```python
def ocr_non_vlm_image_pdf_ingestion_builder(config):

    input_type = config["input_type"]
    ocr_engine = config["ocr_engine"]

    if input_type == "pdf":

        ingestion = PdfToImage() \
            .setInputCol("content") \
            .setOutputCol("image") \
            .setKeepInput(False) \
            .setCompressImage(False) \
            .setResolution(config["resolution"]) \
            .setImageDimsCol("frame_dims")
    else:

        ingestion = BinaryToImage() \
            .setInputCol("content") \
            .setOutputCol("image") \
            .setCompressImage(False) \
            .setImageDimsCol("frame_dims")

    if ocr_engine == "v1":

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

        return [ingestion, ocr]

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

    if ocr_engine == "v2":
        ocr = ImageToTextV2.pretrained("ocr_large_printed_v2_opt", "en", "clinical/ocr") \
            .setRegionsColumn("text_regions") \
            .setInputCols(["image"]) \
            .setOutputCol("text") \
            .setOutputFormat(OcrOutputFormat.TEXT_WITH_POSITIONS) \
            .setGroupImages(False) \
            .setKeepInput(True) \
            .setUseGPU(config["gpu"]) \
            .setUseCaching(True)
    else:
        ocr = ImageToTextV3() \
            .setInputCols(["image", "text_regions"]) \
            .setOutputCol("text")

    return [ingestion, text_detector, ocr]
```

### Image/PDF Config Helpers

```python
def image_pdf_config_cleaner(config):
    config = dict(config)

    for key in ["input_type", "ocr_engine", "detector_engine"]:
        value = config.get(key)
        if isinstance(value, str):
            config[key] = value.lower()

    return config


def image_pdf_required_config_keys(config):
    required_keys = ["input_type", "ocr_engine"]

    if config["input_type"] == "pdf":
        required_keys.append("resolution")

    if config["ocr_engine"] == "vlm":
        required_keys.extend(["nPredict", "line_tolerance", "space_tolerance"])
    elif config["ocr_engine"] in ["v2", "v3"]:
        required_keys.extend(["detector_engine", "gpu"])

    return required_keys


def image_pdf_config_checker(config):
    config = image_pdf_config_cleaner(config)

    missing_base_keys = [key for key in ["input_type", "ocr_engine"] if key not in config]
    if missing_base_keys:
        raise ValueError(f"Missing Image/PDF Pixel Builder config key(s): {missing_base_keys}")

    if config["input_type"] not in ["image", "pdf"]:
        raise ValueError(f"Unsupported Input Type: {config['input_type']!r}. Expected 'image' or 'pdf'.")

    if config["ocr_engine"] not in ["vlm", "v1", "v2", "v3"]:
        raise ValueError(f"Unsupported OCR engine: {config['ocr_engine']!r}. Expected 'VLM', 'V1', 'V2', or 'V3'.")

    required_keys = image_pdf_required_config_keys(config)
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise ValueError(f"Missing Image/PDF Pixel Builder config key(s): {missing_keys}")

    if config["ocr_engine"] in ["v2", "v3"] and config["detector_engine"] not in ["v1", "v2"]:
        raise ValueError(f"Unsupported Detector engine: {config['detector_engine']!r}. Expected 'V1' or 'V2'.")

    return config


```

### Image/PDF VLM Pipeline Builder

Use this builder only when `config["ocr_engine"]` is `VLM`. Do not include the non-VLM helper or branch in the generated code.

```python
def image_pdf_pipeline_builder(spark_session, config, intermediate_result, pretrained_pipeline):
    config = image_pdf_config_checker(config)
    ingestion_stages = ocr_vlm_image_pdf_ingestion_builder(config)

    ner_output, nlp_pipeline = nlp_builder(spark_session, pretrained_pipeline)

    position_finder = PositionFinder() \
        .setInputCols([ner_output]) \
        .setOutputCol("coordinates") \
        .setPageMatrixCol("positions")

    final_stages = [*ingestion_stages, nlp_pipeline, position_finder]

    schema_converter_internal = ImageSchemaConverter() \
        .setInputCol("image_assembler") \
        .setOutputCol("image") \
        .setOutputSchema(ImageSchemaConversion.INTERNAL) \
        .setKeepInput(False)

    final_stages.extend([schema_converter_internal])

    draw_regions = ImageDrawRegions() \
        .setInputCol("image") \
        .setInputRegionsCol("coordinates") \
        .setRectColor(Color.black) \
        .setFilledRect(True) \
        .setOutputCol("image_with_regions")

    final_stages.extend([draw_regions])

    if intermediate_result:
        return PipelineModel(stages=final_stages)

    if config["input_type"] == "pdf":

        img_to_pdf = ImageToPdf() \
            .setPageNumCol("pagenum") \
            .setOriginCol("path") \
            .setOutputCol("pdf") \
            .setInputCol("image_with_regions") \
            .setAggregatePages(True)

        final_stages.extend([img_to_pdf])

    return PipelineModel(stages=final_stages)
```

### Image/PDF Non-VLM Pipeline Builder

Use this builder only when `config["ocr_engine"]` is `V1`, `V2`, or `V3`. Do not include the VLM helper or branch in the generated code.

```python
def image_pdf_pipeline_builder(spark_session, config, intermediate_result, pretrained_pipeline):
    config = image_pdf_config_checker(config)
    ingestion_stages = ocr_non_vlm_image_pdf_ingestion_builder(config)

    ner_output, nlp_pipeline = nlp_builder(spark_session, pretrained_pipeline)

    position_finder = PositionFinder() \
        .setInputCols([ner_output]) \
        .setOutputCol("coordinates") \
        .setPageMatrixCol("positions")

    final_stages = [*ingestion_stages, nlp_pipeline, position_finder]

    draw_regions = ImageDrawRegions() \
        .setInputCol("image") \
        .setInputRegionsCol("coordinates") \
        .setRectColor(Color.black) \
        .setFilledRect(True) \
        .setOutputCol("image_with_regions")

    final_stages.extend([draw_regions])

    if intermediate_result:
        return PipelineModel(stages=final_stages)

    if config["input_type"] == "pdf":

        img_to_pdf = ImageToPdf() \
            .setPageNumCol("pagenum") \
            .setOriginCol("path") \
            .setOutputCol("pdf") \
            .setInputCol("image_with_regions") \
            .setAggregatePages(True)

        final_stages.extend([img_to_pdf])

    return PipelineModel(stages=final_stages)
```

### Image/PDF Config Examples

Use one config flavor: `image_vlm`, `image_v1`, `image_detected`, `pdf_vlm`, `pdf_v1`, or `pdf_detected`. VLM configs require a `caption` column. V1 configs use `ImageToText` without detection. Detected configs use `ImageTextDetector` or `ImageTextDetectorV2` with `ImageToTextV2` or `ImageToTextV3`. Non-VLM configs do not use `caption`, `nPredict`, `line_tolerance`, or `space_tolerance`.

Image VLM config:

```python
config = {
    "input_type": "Image",
    "ocr_engine": "VLM",
    "nPredict": 8000,
    "line_tolerance": 15,
    "space_tolerance": 15,
}
```

Image V1 config:

```python
config = {
    "input_type": "Image",
    "ocr_engine": "V1",
}
```

Image detected non-VLM config:

```python
config = {
    "input_type": "Image",
    "gpu": True,
    "ocr_engine": "V2",
    "detector_engine": "V1",
}
```

PDF VLM config:

```python
config = {
    "input_type": "PDF",
    "resolution": 300,
    "ocr_engine": "VLM",
    "nPredict": 8000,
    "line_tolerance": 15,
    "space_tolerance": 15,
}
```

PDF V1 config:

```python
config = {
    "input_type": "PDF",
    "resolution": 300,
    "ocr_engine": "V1",
}
```

PDF detected non-VLM config:

```python
config = {
    "input_type": "PDF",
    "resolution": 300,
    "gpu": True,
    "ocr_engine": "V2",
    "detector_engine": "V1",
}
```

### Image VLM Usage Example

```python
from sparknlp.pretrained import PretrainedPipeline
from pyspark.sql.functions import lit

config = {
    "input_type": "Image",
    "ocr_engine": "VLM",
    "nPredict": 8000,
    "line_tolerance": 15,
    "space_tolerance": 15,
}

clinical_pipeline_name = "clinical_deidentification_docwise_benchmark_medium"
deid_pipeline = PretrainedPipeline(clinical_pipeline_name, "en", "clinical/models")

pipeline = image_pdf_pipeline_builder(
    spark_session=spark,
    config=config,
    intermediate_result=False,
    pretrained_pipeline=deid_pipeline,
)

image_path = "/path/to/image/files"
image_df = spark.read.format("binaryFile").load(image_path)
vlm_prompt = "Detect and recognize text in the image, and output the text coordinates in a formatted manner."
image_with_prompt_df = image_df.withColumn("caption", lit(vlm_prompt))

result = pipeline.transform(image_with_prompt_df).cache()
display_images(df=result, field="image_with_regions", limit=5, width=600, show_meta=True)
display_images_horizontal(df=result, fields="image,image_with_regions", limit=5, width=700, show_meta=True)
saved_paths = save_image_to_disk(result, image_col="image_with_regions", output_dir="/tmp/image_deid")
saved_paths[:5]
```

### Image Non-VLM Usage Note

For non-VLM image usage, use the image V1 or image detected config above, skip the `caption` column, and transform `image_df` directly:

```python
pipeline = image_pdf_pipeline_builder(
    spark_session=spark,
    config=config,
    intermediate_result=False,
    pretrained_pipeline=deid_pipeline,
)

image_path = "/path/to/image/files"
image_df = spark.read.format("binaryFile").load(image_path)
result = pipeline.transform(image_df).cache()
```

### PDF VLM Usage Example

```python
from sparknlp.pretrained import PretrainedPipeline
from pyspark.sql.functions import lit

config = {
    "input_type": "PDF",
    "resolution": 300,
    "ocr_engine": "VLM",
    "nPredict": 8000,
    "line_tolerance": 15,
    "space_tolerance": 15,
}

clinical_pipeline_name = "clinical_deidentification_docwise_benchmark_medium"
deid_pipeline = PretrainedPipeline(clinical_pipeline_name, "en", "clinical/models")

pipeline = image_pdf_pipeline_builder(
    spark_session=spark,
    config=config,
    intermediate_result=False,
    pretrained_pipeline=deid_pipeline,
)

pdf_path = "/path/to/pdf/files"
pdf_df = spark.read.format("binaryFile").load(pdf_path)
vlm_prompt = "Detect and recognize text in the image, and output the text coordinates in a formatted manner."
pdf_with_prompt_df = pdf_df.withColumn("caption", lit(vlm_prompt))

result = pipeline.transform(pdf_with_prompt_df).cache()
display_pdf(df=result, field="pdf", limit=5, width=700, show_meta=True)
saved_paths = save_pdf_to_disk(result, pdf_col="pdf", output_dir="/tmp/pdf_deid")
saved_paths[:5]
```

### PDF Non-VLM Usage Note

For non-VLM PDF usage, use the PDF V1 or PDF detected config above, skip the `caption` column, and transform `pdf_df` directly:

```python
pipeline = image_pdf_pipeline_builder(
    spark_session=spark,
    config=config,
    intermediate_result=False,
    pretrained_pipeline=deid_pipeline,
)

pdf_path = "/path/to/pdf/files"
pdf_df = spark.read.format("binaryFile").load(pdf_path)
result = pipeline.transform(pdf_df).cache()
```

### Optional Image/PDF VLM Inspection Pass

Include this block only when the user explicitly asks for intermediate results. This VLM inspection pipeline stops before PDF reconstruction, so selecting `text`, `coordinates`, and `image_with_regions` is valid here. For non-VLM inspection, use the matching non-VLM config and transform the original `image_df` or `pdf_df` directly.

```python
intermediate_pipeline = image_pdf_pipeline_builder(
    spark_session=spark,
    config=config,
    intermediate_result=True,
    pretrained_pipeline=deid_pipeline,
)

input_df = pdf_with_prompt_df
intermediate_df = intermediate_pipeline.transform(input_df).cache()
display_intermediate_result(
    intermediate_df,
    columns=["text", "coordinates", "image_with_regions"],
    limit=10,
    truncate=False,
)
```

### Image/PDF Completion Notes

Keep the same action shape as the DICOM section when extending examples:

1. Define image/PDF OCR builder helpers.
2. Use `nlp_builder(...)` from `common_pixel_builder_helpers`.
3. Define the image/PDF pixel pipeline builder.
4. Include model loading, transform call, display, output rebuild, and save logic. Include an inspection pass only when the user explicitly asks for intermediate results.
5. Keep code examples copy-ready for notebook use.
