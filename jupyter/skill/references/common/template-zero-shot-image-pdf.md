# Image/PDF Zero-Shot Template

Use this file for shared standalone image and PDF zero-shot OCR ingestion code.

Before this section, include `common_zero_shot_config` and `common_zero_shot_ner_builder` from `common/template-zero-shot-builder.md`, plus `common_zero_shot_position_finder` from `common/visual_stage.md`. The ingestion helpers below handle both input types through `config["input_type"]`.

Pick `config["ocr_engine"]` using `models.yaml` `ocr_engine.selection_guide.default_by_hardware` — GPU defaults to VLM, CPU defaults to V1 (V3 or V2 only if requested). Output only the OCR helper and branch needed by the selected engine: for VLM configs, include `ocr_vlm_zero_shot_image_pdf_builder(...)`; do not output the non-VLM helper. For non-VLM configs, include only `ocr_non_vlm_zero_shot_image_pdf_builder(...)`; do not output the VLM helper.

## image_pdf_zero_shot_ocr

### Image/PDF Zero-Shot VLM OCR Helper

```python
def ocr_vlm_zero_shot_image_pdf_builder(config):

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

### Image/PDF Zero-Shot Non-VLM OCR Helper

```python
def ocr_non_vlm_zero_shot_image_pdf_builder(config):

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

### Image/PDF Zero-Shot Config Helpers

```python
def zero_shot_image_pdf_config_cleaner(config):
    config = dict(config)

    for key in ["input_type", "ocr_engine", "detector_engine"]:
        value = config.get(key)
        if isinstance(value, str):
            config[key] = value.lower()

    return config


def zero_shot_image_pdf_required_config_keys(config):
    required_keys = ["input_type", "ocr_engine"]

    if config["input_type"] == "pdf":
        required_keys.append("resolution")

    if config["ocr_engine"] == "vlm":
        required_keys.extend(["nPredict", "line_tolerance", "space_tolerance"])
    elif config["ocr_engine"] in ["v2", "v3"]:
        required_keys.extend(["detector_engine", "gpu"])

    return required_keys


def zero_shot_image_pdf_config_checker(config):
    config = zero_shot_image_pdf_config_cleaner(config)

    missing_base_keys = [key for key in ["input_type", "ocr_engine"] if key not in config]
    if missing_base_keys:
        raise ValueError(f"Missing Image/PDF Zero-Shot config key(s): {missing_base_keys}")

    if config["input_type"] not in ["image", "pdf"]:
        raise ValueError(f"Unsupported Input Type: {config['input_type']!r}. Expected 'image' or 'pdf'.")

    if config["ocr_engine"] not in ["vlm", "v1", "v2", "v3"]:
        raise ValueError(f"Unsupported OCR engine: {config['ocr_engine']!r}. Expected 'VLM', 'V1', 'V2', or 'V3'.")

    required_keys = zero_shot_image_pdf_required_config_keys(config)
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise ValueError(f"Missing Image/PDF Zero-Shot config key(s): {missing_keys}")

    if config["ocr_engine"] in ["v2", "v3"] and config["detector_engine"] not in ["v1", "v2"]:
        raise ValueError(f"Unsupported Detector engine: {config['detector_engine']!r}. Expected 'V1' or 'V2'.")

    return config
```
