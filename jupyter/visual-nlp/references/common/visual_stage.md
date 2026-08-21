# Visual Stages

Use this file for visual coordinate conversion, coordinate finding, and image/PDF drawing stages shared by DICOM, image, and PDF routes.

Keep OCR recognizers in `common/stages-ocr.md`. Keep NER model wiring in `common/stages-ner.md`. Keep DICOM-specific drawing stages in `dicom/stages.md`.

## DocumentCoordinatesToText

Use this after VLM OCR to convert OCR completions into text, regions, and page matrix coordinates.

Parameters:

- `setRegionCol(...)`: Names the output regions/coordinates column; default is `regions`.
- `setPageMatrixCol(...)`: Names the output character-level mapping column; default is `positions`.
- `setLineTolerance(...)`: Pixel gap used when grouping text regions into lines; default is `15`.
- `setSpaceTolerance(...)`: Pixel gap used when grouping text regions into words; default is `15`.
- `setImageDimsCol(...)`: Names the image dimensions column containing width and height.

```python
coordinate_extract = DocumentCoordinatesToText() \
    .setInputCol("completions") \
    .setImageDimsCol("frame_dims") \
    .setOutputCol("text") \
    .setPageMatrixCol("positions") \
    .setRegionCol("regions") \
    .setLineTolerance(config["line_tolerance"]) \
    .setSpaceTolerance(config["space_tolerance"])
```

## PositionFinder

Use `PositionFinder` after NER when OCR produced a `positions` page matrix.

For V1 OCR, `ImageToText` must set `setPositionsCol("positions")` before `PositionFinder` consumes that page matrix. For VLM OCR, `DocumentCoordinatesToText` must set `setPageMatrixCol("positions")`.

```python
position_finder = PositionFinder() \
    .setInputCols(["merged_ner_chunk"]) \
    .setOutputCol("coordinates") \
    .setPageMatrixCol("positions") \
    .setIgnoreSchema(False) \
    .setOcrScaleFactor(1.1)
```

## common_zero_shot_position_finder

Use this after `build_stacked_zero_shot_ner_pipeline(...)` when the OCR stage produced a `positions` page matrix.

```python
position_finder = PositionFinder() \
    .setInputCols(["merged_ner_chunk"]) \
    .setOutputCol("coordinates") \
    .setPageMatrixCol("positions") \
    .setIgnoreSchema(False) \
    .setOcrScaleFactor(1.1)
```

## ImageDrawRegions

Use `ImageDrawRegions` for standalone image redaction, standalone PDF page-image redaction, and DICOM encapsulated PDF page-image redaction. Do not use it for ordinary DICOM pixel redaction; use the DICOM-specific drawing stage documented in `dicom/stages.md`.

Parameters:

- `setInputRegionsCol(...)`: Names the input regions column. Use coordinates from `PositionFinder` or detected text regions from image text detectors; default is `regions`.
- `setRectColor(...)`: Sets the rectangle border or fill color used when redaction is enabled; default is `black`.
- `setFilledRect(...)`: Toggles whether rectangles are filled. Applies only when `setPatchImages(False)`; default is `False`.
- `setPatchImages(...)`: Controls operation type. Use `False` for de-identification/redaction and `True` for obfuscation/text patching; default is `False`.

```python
draw_regions = ImageDrawRegions() \
    .setInputCol("image") \
    .setInputRegionsCol("coordinates") \
    .setRectColor(Color.black) \
    .setFilledRect(True) \
    .setPatchImages(False) \
    .setOutputCol("image_with_regions")
```
