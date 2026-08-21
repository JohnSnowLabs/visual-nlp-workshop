# OCR Stages

Use this file for OCR stage choices shared by DICOM, image, and PDF routes.

## MedicalVisionLLM

Use VLM OCR as the default highest-accuracy path unless the user asks for cheaper OCR.

Every `MedicalVisionLLM` stage must use:

```python
# nPredict: DICOM frames 1000-2000 (e.g. 1024); dense PDF pages 4000+ (e.g. 4000).
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
```

Set `config["nPredict"]` based on how much text the page/frame is expected to contain:

- DICOM frames: low is fine, `1000`-`2000` (e.g. `1024`).
- Dense PDF pages (standalone PDF or DICOM encapsulated PDF): high, `4000+` (e.g. `4000`), since a full page of dense text needs far more output tokens than a DICOM frame.

Never leave `nPredict` at a DICOM-frame-sized default for a dense-PDF-page workflow.

## ImageToText

Use this for lightweight non-detection OCR when `config["ocr_engine"] == "v1"`. `ImageToText` generates the `positions` page matrix used by `PositionFinder`, so V1 redaction paths must set `setPositionsCol("positions")`.

Never suggest `ImageToText` (V1) for normal DICOM pixel routes (`dicom.pixel_phi_builder`, `dicom.pixel_phi_zero_shot`, `dicom.pixel_remove_all_text`). Those only support `VLM`, `V2`, or `V3` (see `dicom/template-pixel-builder.md`); when one of them needs the cheapest non-VLM path, always suggest `ImageToTextV3` instead. DICOM encapsulated PDF routes (`dicom.encapsulated_pdf_phi_vlm`, `dicom.encapsulated_pdf_phi_builder`) are the one exception and may suggest `ImageToText` (V1). Outside that exception, `ImageToText` (V1) may only be suggested for image or PDF routes, and only when the user explicitly asks for it.

Parameters:

- `setInputCol(...)`: Names the input image column.
- `setOutputCol(...)`: Names the output text column.
- `setPositionsCol(...)`: Names the output positions column for `PositionFinder`; use `positions`.
- `setIncludeConfidence(...)`: Enables or disables confidence computation.
- `setConfidenceCol(...)`: Names the confidence output column.
- `setConfidenceThreshold(...)`: Sets the OCR confidence threshold.
- `setIgnoreResolution(...)`: Ignores image metadata resolution when enabled.
- `setOcrParams(...)`: Passes OCR parameters as `key=value` strings.
- `setPdfCoordinates(...)`: Converts positions into PDF point coordinates when enabled.
- `setWithSpaces(...)`: Includes spaces in output positions.
- `setKeepLayout(...)`: Preserves text layout in the result.
- `setOutputSpaceCharacterWidth(...)`: Sets output space character width in points for layout keeping.

```python
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
```

## ImageToTextV2

Use this after text detection when `config["ocr_engine"] == "v2"`.

Parameters:

- `setGroupImages(...)`: Controls whether images are grouped for OCR processing.
- `setRegionsColumn(...)`: Names the input column containing detected regions to process.
- `setLineTolerance(...)`: Pixel distance used to group text regions into the same line.
- `setUseCaching(...)`: Enables caching to speed up repeated processing.
- `setUseGPU(...)`: Enables GPU execution when GPU resources are available.
- `setKeepInput(True)`: Required for PHI identification, redaction, or side-by-side image inspection because downstream stages still need the `image` column.
- `setKeepInput(False)`: Allowed only for OCR-only text extraction when no later stage needs the `image` column.

```python
ocr = ImageToTextV2.pretrained("ocr_large_printed_v2_opt", "en", "clinical/ocr") \
    .setRegionsColumn("text_regions") \
    .setInputCols(["image"]) \
    .setOutputCol("text") \
    .setOutputFormat(OcrOutputFormat.TEXT_WITH_POSITIONS) \
    .setGroupImages(False) \
    .setKeepInput(True) \
    .setUseGPU(config["gpu"]) \
    .setUseCaching(True)
```

## ImageToTextV3

Use this after text detection when `config["ocr_engine"] == "v3"`.

```python
ocr = ImageToTextV3() \
    .setInputCols(["image", "text_regions"]) \
    .setOutputCol("text")
```

## Compression Rule

- Use compressed image schema only when the image is consumed by `MedicalVisionLLM`.
- Do not use compressed image schema for `ImageToText`, `ImageToTextV2`, `ImageToTextV3`, or text detection stages.
- For VLM OCR, input-specific DICOM, image, and PDF extraction stages should use `setCompressImage(True)` and write dimensions to `frame_dims`.
- For image and PDF inputs, `BinaryToImage` and `PdfToImage` use `setImageDimsCol("frame_dims")`.
- For non-VLM OCR, set `setCompressImage(False)` on image extraction when the stage supports it.
