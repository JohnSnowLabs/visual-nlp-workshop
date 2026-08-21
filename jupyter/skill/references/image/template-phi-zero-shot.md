# Image PHI Zero-Shot Template

Use this route when the user asks for configurable entity coverage or stacked zero-shot NER on standalone image files.

Expected architecture:

1. Define OCR, zero-shot NER, visual, and redaction stages in one pipeline block.
2. Load image files after the pipeline block.
3. Add the VLM prompt column (VLM only).
4. Transform the image DataFrame.
5. Display and save final image outputs.

Do not use the common pixel builder for this zero-shot image route.

Keep image loading, OCR, `ImageDrawRegions`, image display, and image saving in this image template. Keep DICOM-specific stages out of this route.

Before this section, include `common_zero_shot_config` and `common_zero_shot_ner_builder` from `common/template-zero-shot-builder.md`, plus `common_zero_shot_position_finder` from `common/visual_stage.md`, plus `image_pdf_zero_shot_ocr` from `common/template-zero-shot-image-pdf.md`.

Pick `config["ocr_engine"]` using `models.yaml` `ocr_engine.selection_guide.default_by_hardware` — GPU defaults to VLM, CPU defaults to V1 (V3 or V2 only if requested). Output only the OCR helper and branch needed by the selected engine.

## image_pixel_phi_zero_shot

### Image Zero-Shot VLM Usage Example

```python
from pyspark.sql.functions import lit

config.update({"input_type": "image", "ocr_engine": "VLM", "nPredict": 1024, "line_tolerance": 5, "space_tolerance": 15})
config = zero_shot_image_pdf_config_checker(config)

ingestion_stages = ocr_vlm_zero_shot_image_pdf_builder(config)
phi_detection_model = build_stacked_zero_shot_ner_pipeline(input_text="text", output_chunk="merged_ner_chunk")

schema_converter_internal = ImageSchemaConverter() \
    .setInputCol("image_assembler") \
    .setOutputCol("image") \
    .setOutputSchema(ImageSchemaConversion.INTERNAL) \
    .setKeepInput(False)

draw_regions = ImageDrawRegions() \
    .setInputCol("image") \
    .setInputRegionsCol("coordinates") \
    .setRectColor(Color.black) \
    .setFilledRect(True) \
    .setOutputCol("image_with_regions")

pipeline = PipelineModel(stages=[
    *ingestion_stages,
    phi_detection_model,
    position_finder,
    schema_converter_internal,
    draw_regions
])

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

### Optional Image Inspection Pass

Include this block only when the user explicitly asks to inspect OCR text, detected regions, or PHI coordinates before final save/display.

```python
inspection_pipeline = PipelineModel(stages=[
    *ingestion_stages,
    phi_detection_model,
    position_finder
])

inspection_df = inspection_pipeline.transform(image_with_prompt_df).cache()
display_intermediate_result(inspection_df, columns=["text", "regions", "coordinates"], limit=10, truncate=False)
```

### Image Zero-Shot Non-VLM Usage Note

For non-VLM usage (including `V1`), skip `caption`/`nPredict`/`line_tolerance`/`space_tolerance`, build with `ocr_non_vlm_zero_shot_image_pdf_builder(...)` instead, skip `schema_converter_internal` since the image never left `INTERNAL` schema, and transform `image_df` directly:

```python
config.update({"input_type": "image", "ocr_engine": "V1"})
config = zero_shot_image_pdf_config_checker(config)

ingestion_stages = ocr_non_vlm_zero_shot_image_pdf_builder(config)
phi_detection_model = build_stacked_zero_shot_ner_pipeline(input_text="text", output_chunk="merged_ner_chunk")

draw_regions = ImageDrawRegions() \
    .setInputCol("image") \
    .setInputRegionsCol("coordinates") \
    .setRectColor(Color.black) \
    .setFilledRect(True) \
    .setOutputCol("image_with_regions")

pipeline = PipelineModel(stages=[
    *ingestion_stages,
    phi_detection_model,
    position_finder,
    draw_regions
])

image_path = "/path/to/image/files"
image_df = spark.read.format("binaryFile").load(image_path)
result = pipeline.transform(image_df).cache()
display_images(df=result, field="image_with_regions", limit=5, width=600, show_meta=True)

saved_paths = save_image_to_disk(result, image_col="image_with_regions", output_dir="/tmp/image_deid")
saved_paths[:5]
```
