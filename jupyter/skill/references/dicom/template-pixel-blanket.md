# Blanket Text Redaction Template

## pixel_remove_all_text

`DicomDrawRegions` is an aggregation stage. Use a separate inspection pipeline that stops before `DicomDrawRegions` if the user wants `text_regions`; after this final stage, validate with `display_dicom(...)` on `dicom_pixel_cleaned`.

```python
config = {
    "text_detector": "ImageTextDetector",
    "use_gpu": True,
    "score_threshold": 0.5,
    "text_threshold": 0.2,
    "size_threshold": 10,
    "with_refiner": True,
    "link_threshold": 0.5,
    "scale": 1.0,
    "frame_sampling": 5,
    "frame_sampling_strategy": FrameSamplingStrategy.CONSECUTIVE,
    "compression_mode": "disabled",
    "compression_quality": 80,
    "memory_optimized": False,
    "text_regions_col": "text_regions",
    "final_dicom_col": "dicom_pixel_cleaned",
}

dicom_to_image = DicomToImageV3() \
    .setInputCols(["content"]) \
    .setOutputCol("image") \
    .setKeepInput(False) \
    .setScale(config["scale"]) \
    .setFrameLimit(config["frame_sampling"]) \
    .setFrameSamplingStrategy(config["frame_sampling_strategy"]) \
    .setCompressImage(False) \
    .setCompressionMode(config["compression_mode"]) \
    .setCompressionQuality(config["compression_quality"]) \
    .setMemoryOptimized(config["memory_optimized"])

if config["text_detector"] == "ImageTextDetector":
    
    text_detector = ImageTextDetector.pretrained("image_text_detector_mem_opt", "en", "clinical/ocr") \
        .setInputCol("image") \
        .setOutputCol(config["text_regions_col"]) \
        .setScoreThreshold(config["score_threshold"]) \
        .setLinkThreshold(config["link_threshold"]) \
        .setTextThreshold(config["text_threshold"]) \
        .setSizeThreshold(config["size_threshold"]) \
        .setWithRefiner(config["with_refiner"]) \
        .setUseGPU(config["use_gpu"])

elif config["text_detector"] == "ImageTextDetectorV2":
    text_detector = ImageTextDetectorV2.pretrained("image_text_detector_v2", "en", "clinical/ocr") \
        .setInputCol("image") \
        .setOutputCol(config["text_regions_col"]) \
        .setScoreThreshold(config["score_threshold"]) \
        .setTextThreshold(config["text_threshold"]) \
        .setSizeThreshold(config["size_threshold"]) \
        .setWithRefiner(config["with_refiner"]) \
        .setUseGPU(config["use_gpu"])

else:
    raise ValueError(f"Unsupported text_detector: {config['text_detector']!r}")

draw_regions = DicomDrawRegions() \
    .setInputCol("path") \
    .setInputRegionsCol(config["text_regions_col"]) \
    .setOutputCol(config["final_dicom_col"]) \
    .setAggCols(["path"]) \
    .setKeepInput(True) \
    .setScaleFactor(1 / config["scale"])

pipeline = PipelineModel(stages=[dicom_to_image, text_detector, draw_regions])

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)
result = pipeline.transform(dicom_df).cache()
display_dicom(df=result, fields=config["final_dicom_col"], limit=1, width=300)

saved_paths = save_dicom_to_disk(result, dicom_col=config["final_dicom_col"], output_dir="/tmp/dicom_deid")
saved_paths[:5]
```
