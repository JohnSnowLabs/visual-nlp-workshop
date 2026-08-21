# Image Stage Contracts

Use this file for image-file workflows only.

Current status: starter contracts. Before generating production-ready image code, verify exact stage parameters against local workshop examples or official John Snow Labs / Spark OCR documentation for the installed version.

Expected route shape:

1. Define the image pipeline first, then load image files into a Spark DataFrame before `pipeline.transform(...)`.
2. Convert binary image content into Spark OCR image schema.
3. Run OCR or text detection.
4. For PHI-only redaction, run NER and position finding.
5. Draw/redact image regions.
6. Save or display final image outputs.

Keep image routes separate from DICOM routes. Do not use DICOM metadata stages or `DicomDrawRegions` for ordinary image files.

Image schema rule:

- Use `BinaryToImage().setCompressImage(True).setImageDimsCol("frame_dims")` only for `MedicalVisionLLM`.
- Use regular image schema for `ImageToText`, `ImageToTextV2`, `ImageToTextV3`, and text detection.
- Set `setCompressImage(False)` for non-VLM image extraction when the installed stage supports it.

## BinaryToImage

Role: Convert binary image file content into Spark OCR image schema rows for OCR, text detection, or redaction.

Key params:

- `setInputCol(...)`: Names the input image bytes column.
- `setOutputCol(...)`: Names the output image column.
- `setOriginCol(...)`: Names the input column containing the original file path.
- `setPageNumCol(...)`: Names the output image number column.
- `setKeepInput(...)`: Keeps or drops the input column.
- `setImageType(...)`: Sets the output image type in `BufferedImage` format.
- `setImageDimsCol(...)`: Names the image dimension column containing width and height.
- `setCompressImage(...)`: Returns compressed image file bytes in `image.data` instead of decoded bitmap data when enabled.

VLM example:

```python
binary_to_image = BinaryToImage() \
    .setInputCol("content") \
    .setOutputCol("image") \
    .setCompressImage(True) \
    .setImageDimsCol("frame_dims")
```

Non-VLM example:

```python
binary_to_image = BinaryToImage() \
    .setInputCol("content") \
    .setOutputCol("image") \
    .setCompressImage(False) \
    .setImageDimsCol("frame_dims")
```
