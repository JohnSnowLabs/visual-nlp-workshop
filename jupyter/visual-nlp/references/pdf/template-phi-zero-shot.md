# PDF PHI Zero-Shot Template

Use this route when the user asks for configurable entity coverage or stacked zero-shot NER on standalone PDF files.

Expected architecture:

1. Define PDF rendering, OCR, zero-shot NER, visual, redaction, and PDF reconstruction stages in one pipeline block.
2. Load PDF files after the pipeline block.
3. Add the VLM prompt column (VLM only).
4. Transform the PDF DataFrame.
5. Display and save final PDF outputs.

Do not use the common pixel builder for this zero-shot PDF route.

Keep PDF loading, page rendering, `ImageDrawRegions`, PDF reconstruction, PDF display, and PDF saving in this PDF template. Keep DICOM-specific stages out of this route.

Before this section, include `common_zero_shot_config` and `common_zero_shot_ner_builder` from `common/template-zero-shot-builder.md`, plus `common_zero_shot_position_finder` from `common/visual_stage.md`, plus `image_pdf_zero_shot_ocr` from `common/template-zero-shot-image-pdf.md`.

Pick `config["ocr_engine"]` using `models.yaml` `ocr_engine.selection_guide.default_by_hardware` — GPU defaults to VLM, CPU defaults to V1 (V3 or V2 only if requested). Output only the OCR helper and branch needed by the selected engine.

## pdf_text_phi_zero_shot

### PDF Zero-Shot VLM Usage Example

```python
from pyspark.sql.functions import lit

config.update({"input_type": "pdf", "resolution": 300, "ocr_engine": "VLM", "nPredict": 1024, "line_tolerance": 5, "space_tolerance": 15})
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

img_to_pdf = ImageToPdf() \
    .setPageNumCol("pagenum") \
    .setOriginCol("path") \
    .setOutputCol("pdf") \
    .setInputCol("image_with_regions") \
    .setAggregatePages(True)

pipeline = PipelineModel(stages=[
    *ingestion_stages,
    phi_detection_model,
    position_finder,
    schema_converter_internal,
    draw_regions,
    img_to_pdf
])

pdf_path = "/path/to/pdf/files"
pdf_df = spark.read.format("binaryFile").load(pdf_path)
vlm_prompt = "Detect and recognize text in the image, and output the text coordinates in a formatted manner."
pdf_with_prompt_df = pdf_df.withColumn("caption", lit(vlm_prompt))

result = pipeline.transform(pdf_with_prompt_df).cache()
display_pdf(df=result, field="pdf", limit=5, width=700, show_meta=True)

saved_paths = save_pdf_to_disk(result, pdf_col="pdf", output_dir="/tmp/pdf_deid")
saved_paths[:5]
```

### Optional PDF Inspection Pass

Include this block only when the user explicitly asks to inspect OCR text, detected regions, PHI coordinates, or redacted page images before final PDF reconstruction.

```python
inspection_pipeline = PipelineModel(stages=[
    *ingestion_stages,
    phi_detection_model,
    position_finder,
    schema_converter_internal,
    draw_regions
])

inspection_df = inspection_pipeline.transform(pdf_with_prompt_df).cache()
display_intermediate_result(
    inspection_df,
    columns=["text", "regions", "coordinates", "image_with_regions"],
    limit=10,
    truncate=False,
)
```

### PDF Zero-Shot Non-VLM Usage Note

For non-VLM usage (including `V1`), skip `caption`/`nPredict`/`line_tolerance`/`space_tolerance`, build with `ocr_non_vlm_zero_shot_image_pdf_builder(...)` instead, skip `schema_converter_internal` since the image never left `INTERNAL` schema, and transform `pdf_df` directly:

```python
config.update({"input_type": "pdf", "resolution": 300, "ocr_engine": "V1"})
config = zero_shot_image_pdf_config_checker(config)

ingestion_stages = ocr_non_vlm_zero_shot_image_pdf_builder(config)
phi_detection_model = build_stacked_zero_shot_ner_pipeline(input_text="text", output_chunk="merged_ner_chunk")

draw_regions = ImageDrawRegions() \
    .setInputCol("image") \
    .setInputRegionsCol("coordinates") \
    .setRectColor(Color.black) \
    .setFilledRect(True) \
    .setOutputCol("image_with_regions")

img_to_pdf = ImageToPdf() \
    .setPageNumCol("pagenum") \
    .setOriginCol("path") \
    .setOutputCol("pdf") \
    .setInputCol("image_with_regions") \
    .setAggregatePages(True)

pipeline = PipelineModel(stages=[
    *ingestion_stages,
    phi_detection_model,
    position_finder,
    draw_regions,
    img_to_pdf
])

pdf_path = "/path/to/pdf/files"
pdf_df = spark.read.format("binaryFile").load(pdf_path)
result = pipeline.transform(pdf_df).cache()
display_pdf(df=result, field="pdf", limit=5, width=700, show_meta=True)

saved_paths = save_pdf_to_disk(result, pdf_col="pdf", output_dir="/tmp/pdf_deid")
saved_paths[:5]
```
