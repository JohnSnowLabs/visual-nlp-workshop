# PDF Stage Contracts

Use this file for PDF-file workflows only.

Current status: starter contracts. Before generating production-ready PDF code, verify exact stage parameters against local workshop examples or official John Snow Labs / Spark OCR documentation for the installed version.

Expected route shape:

1. Define the PDF pipeline first, then load PDF files into a Spark DataFrame before `pipeline.transform(...)`.
2. Convert PDF pages to images.
3. Run OCR or text detection on page images.
4. For PHI-only redaction, run NER and position finding.
5. Draw/redact regions on page images.
6. Rebuild the PDF and save the final output.

Keep standalone PDF routes separate from DICOM encapsulated PDF routes. DICOM encapsulated PDF workflows belong under `dicom/`.

Image schema rule:

- Use `PdfToImage().setCompressImage(True).setImageDimsCol("frame_dims")` only for `MedicalVisionLLM`.
- Use regular image schema for `ImageToText`, `ImageToTextV2`, `ImageToTextV3`, and text detection.
- Set `setCompressImage(False)` for non-VLM PDF page extraction.

## PdfToImage

Role: Convert PDF file bytes into page image rows for OCR, text detection, redaction, or PDF reconstruction.

Key params:

- `setInputCol(...)`: Names the input PDF bytes column.
- `setOutputCol(...)`: Names the output image column.
- `setSplitPage(...)`: Enables or disables splitting the document into per-page rows.
- `setPageNumCol(...)`: Names the output page number column.
- `setOriginCol(...)`: Names the input column containing the original file path.
- `setImageType(...)`: Sets the output image type in `BufferedImage` format.
- `setMinSizeBeforeFallback(...)`: Text-size threshold below which the default method falls back to an alternate method.
- `setFallBackCol(...)`: Names the column containing fallback parsed text.
- `setResolution(...)`: Sets output image resolution.
- `setPartitionNum(...)`: Sets number of partitions before page splitting.
- `setPartitionNumAfterSplit(...)`: Sets number of partitions after page splitting.
- `setKeepInput(...)`: Keeps or drops the input column.
- `setSplitNumBatch(...)`: Sets split batch partition count.
- `setBinarization(...)`: Enables or disables binarization.
- `setBinarizationParams(...)`: Sets binarization parameters as `key=value` strings.
- `setSplittingStategy(...)`: Sets splitting strategy. Keep this spelling if it is the installed API name.
- `setMaxImageWidth(...)`: Caps output image width in pixels and scales height proportionally; `0` means no resizing.
- `setImageDimsCol(...)`: Names the image dimension column containing width and height.
- `setCompressImage(...)`: Returns compressed image file bytes in `image.data` instead of decoded bitmap data when enabled.

VLM example:

```python
pdf_to_image = PdfToImage() \
    .setInputCol("content") \
    .setOutputCol("image") \
    .setKeepInput(False) \
    .setCompressImage(True) \
    .setResolution(config["resolution"]) \
    .setImageDimsCol("frame_dims")
```

Non-VLM example:

```python
pdf_to_image = PdfToImage() \
    .setInputCol("content") \
    .setOutputCol("image") \
    .setKeepInput(False) \
    .setCompressImage(False) \
    .setResolution(config["resolution"]) \
    .setImageDimsCol("frame_dims")
```
