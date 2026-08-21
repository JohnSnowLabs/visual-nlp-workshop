# Text Detection Stages

Use this file for text-region detection stages shared by DICOM pixel, standalone image, and standalone PDF workflows.

Keep input conversion, drawing, DICOM metadata handling, PDF reconstruction, display, and save code in the selected input-specific template. This file only defines text detector stages that output region columns such as `text_regions`.

## Image Schema Rule

- Text detection stages consume the regular Spark OCR image schema, not the compressed image schema used by `MedicalVisionLLM`.
- Do not enable `setCompressImage(True)` just because a route uses `ImageTextDetector` or `ImageTextDetectorV2`.
- For detector-only, `ImageToText`, `ImageToTextV2`, or `ImageToTextV3` paths, set `setCompressImage(False)` on the input-specific extraction stage when it supports it.

## Detector Parameters

- `setScoreThreshold(...)`: Detection reliability threshold. Raise it to keep only stronger detections; lower it when faint or low-contrast text is being missed.
- `setTextThreshold(...)`: Region/text score threshold. This score represents how likely a pixel is to be the center of a character.
- `setLinkThreshold(...)`: Link/affinity score threshold. This score represents the likelihood of the space between adjacent characters and is used by `ImageTextDetector`.
- `setSizeThreshold(...)`: Height threshold for detected regions. Use a positive value to remove tiny regions, or `-1` when supported to avoid size filtering.
- `setWithRefiner(...)`: Enables the refiner network as a postprocessing step.

## ImageTextDetector

Use this as the default detector unless the user asks for the Python-based V2 detector.

```python
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
```

## ImageTextDetectorV2

Use this when the user asks for V2 or Python-based text detection.

```python
text_detector = ImageTextDetectorV2.pretrained("image_text_detector_v2", "en", "clinical/ocr") \
    .setInputCol("image") \
    .setOutputCol("text_regions") \
    .setScoreThreshold(0.5) \
    .setTextThreshold(0.2) \
    .setSizeThreshold(10) \
    .setWithRefiner(True) \
    .setUseGPU(config["gpu"])
```

## Detector Selection

Use this config-driven selection pattern when the route supports both detector engines.

```python
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
```

## Downstream Contract

- `ImageTextDetector` and `ImageTextDetectorV2` output `text_regions` by default.
- Use detected regions with `ImageToTextV2` by passing `.setRegionsColumn("text_regions")`.
- Use detected regions with `ImageToTextV3` by passing `.setInputCols(["image", "text_regions"])`.
- Use detected regions directly with drawing stages only for remove-all-text workflows.
