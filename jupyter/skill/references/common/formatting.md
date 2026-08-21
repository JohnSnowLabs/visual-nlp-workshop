# Formatting

Use these rules for every generated Spark OCR, Healthcare NLP, and Visual NLP code example.

## Formatting Rules

- Use backslash chaining for Spark OCR and Healthcare NLP stages.
- Keep every `.pretrained(...)` call on one line with model name, language, and remote path.
- Do not split the class and `.pretrained(...)` across lines.
- Keep every `PretrainedPipeline(...)` constructor call on one line with pipeline name, language, and remote path.
- Keep every JSL stage parameter/setter call on one line, including `.setInputCols(...)`, `.setInputCol(...)`, `.setOutputCol(...)`, thresholds, schema params, GPU params, and visual-stage params.
- Keep every Visual NLP display/comparison utility call on one line: `display_dicom(...)`, `build_metadata_df(...)`, `build_metadata_comparison_df(...)`, and `save_dicom_to_disk(...)`.
- Append `.cache()` to every `pipeline.transform(...)` call (including inspection and intermediate pipeline transforms) so the result DataFrame is cached before it is displayed, compared, or saved.
- Before any `Pipeline(...)` or `PipelineModel(...)` code block, give a numbered list describing what each stage does, in stage order.
- When a VLM OCR workflow needs a `vlm_prompt` caption value, always use the exact text `"Detect and recognize text in the image, and output the text coordinates in a formatted manner."`. Do not invent or vary this wording.
- Keep Spark API and DataFrame commands on one line when short; when a Spark chain gets too long, use backslash continuation with each chained API call on its own line.
- Do not split Spark display or inspection calls such as `.select(...)`, `.selectExpr(...)`, or `.show(...)` across multiple argument lines. Use a named list for long column groups and backslash-chain the DataFrame calls.
- Keep file I/O API commands on one line, including `with open(...) as file:`; do not split file path, mode, encoding, or newline arguments across lines.
- Keep `raise ValueError(...)` and other simple exception statements on one line.
- Keep simple enum values, scalar params, variable assignments, and short lists on one line.
- Allow Python function definitions and function calls to span multiple lines when that is clearer and follows normal Python style.
- Keep JSL `.pretrained(...)` and `.set...(...)` calls on one line even when nearby Python function definitions or calls span multiple lines.
- Use backslash continuation for long Spark API chains; do not wrap Spark chains in parentheses.
- Prefer Python coding style for spacing: use four-space indentation, two blank lines before top-level function definitions, and clean blank lines between logical blocks.
- Split long generated answers into labeled code blocks in this order: imports, helpers, config, strategy/mapping setup, pipeline steps summary, pipeline, data loading and prompts, run, display, and save.
- Pipeline builder/helper functions may be split into helper blocks when long. Always present executable pipeline construction as one contiguous `Pipeline` code block: keep stage definitions, pretrained pipeline/model loading used by the pipeline, and `Pipeline(...)`, `PipelineModel(...)`, or `pipeline = ..._pipeline_builder(...)` creation together in one code fence.
- Define strategy CSV content, strategy file writing, group strategy CSV content, group strategy file writing, and `replaceWithMapping` external mapping DataFrames before defining any pipeline or pipeline builder call that consumes them.
- Define runtime data loading, prompt-column creation, transform calls, display, and save after the pipeline block, except when input data must be prepared before the pipeline because it provides a `replaceWithMapping` external mapping column.
- Define long labels, prompts, entity lists, and config values as named variables.
- Allow `config = {...}` and `config.update({...})` dictionaries to span multiple lines, but keep each config key-value entry on one line. Move nested model lists or long values into named variables before assigning `config`.
- Include complete function implementations in generated notebook code. Do not use placeholder bodies, ellipses, or comments that refer to code elsewhere.
- Name the main configuration dictionary `config`.

## Final Code Formatting Gate

- Generate final, formatter-compliant code before returning it. Do not tell the user to adjust formatting later.
- After a task is selected, output the requested notebook code directly. Do not narrate internal validation, route selection, template changes, or why the code differs from another example.
- Do not write phrases such as "the skill's validation changed the example", "the code passes Python syntax validation", "runtime execution still requires...", or "I am packaging the copy-ready example" in generated workflow answers.
- Do not create, package, or mention a reusable file unless the user explicitly asks for a file artifact. Default output is copy-ready notebook blocks in chat.
- Do not show intermediate results, inspection pipelines, `display_intermediate_result(...)`, or upstream `result.select(...).show(...)` calls unless the user explicitly asks to inspect intermediate OCR text, regions, coordinates, or page images.

## Good Pretrained Formatting

These are abbreviated to illustrate the formatting pattern only — one setter per line, all on the same backslash-chained statement. They are not the canonical parameter lists; use `common/stages-ocr.md` (`MedicalVisionLLM`, `ImageToTextV2`), `common/stages-text-detection.md` (`ImageTextDetector`), and `common/visual_stage.md` (`ImageDrawRegions`, `PositionFinder`) for the actual required parameters of each stage.

```python
vlm_ocr = MedicalVisionLLM.pretrained("jsl-ocr-gguf-vlm1", "en", "clinical/ocr") \
    .setInputCols(["caption_document", "image_assembler"]) \
    .setOutputCol("completions") \
    .setNGpuLayers(99) \
    .setTemperature(0.01)
```

```python
text_detector = ImageTextDetector.pretrained("image_text_detector_mem_opt", "en", "clinical/ocr") \
    .setInputCol("image") \
    .setOutputCol("text_regions") \
    .setScoreThreshold(0.7) \
    .setUseGPU(config["gpu"])
```

```python
ocr = ImageToTextV2.pretrained("ocr_large_printed_v2_opt", "en", "clinical/ocr") \
    .setRegionsColumn("text_regions") \
    .setInputCols(["image"]) \
    .setOutputCol("text") \
    .setUseGPU(config["gpu"])
```

## Good Stage Chain Formatting

Use backslash chaining for Spark OCR and Healthcare NLP stages. Keep constructor calls and setters on one line.

```python
document_assembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")
```

```python
position_finder = PositionFinder() \
    .setInputCols(["ner_chunk"]) \
    .setOutputCol("coordinates") \
    .setPageMatrixCol("positions")
```

```python
draw_regions = ImageDrawRegions() \
    .setInputCol("image") \
    .setInputRegionsCol("coordinates") \
    .setRectColor(Color.black) \
    .setOutputCol("image_with_regions")
```

## Bad Pretrained Formatting

Do not split `.pretrained(...)` arguments across lines.

```text
ocr = ImageToTextV2.pretrained(
    "ocr_large_printed_v2_opt",
    "en",
    "clinical/ocr"
) \
    .setInputCols(["image"]) \
    .setOutputCol("text")
```

```text
text_detector = ImageTextDetector.pretrained(
    "image_text_detector_mem_opt",
    "en",
    "clinical/ocr"
) \
    .setInputCol("image") \
    .setOutputCol("text_regions")
```

```text
vlm_ocr = MedicalVisionLLM.pretrained(
    "jsl-ocr-gguf-vlm1",
    "en",
    "clinical/ocr"
) \
    .setInputCols(["caption_document", "image_assembler"]) \
    .setOutputCol("completions")
```

## Bad Setter Formatting

Do not split JSL stage parameter/setter calls across lines.

```text
text_detector = ImageTextDetector.pretrained("image_text_detector_mem_opt", "en", "clinical/ocr") \
    .setScoreThreshold(
        0.7
    ) \
    .setInputCols(
        ["image"]
    )
```

## Good Spark API Formatting

Keep short Spark commands on one line. Use backslash continuation for long Spark chains. Keep `.select(...)`, `.selectExpr(...)`, and `.show(...)` calls on one line; use named lists for long column groups.

```python
dicom_df = spark.read.format("binaryFile").load(dicom_path)

external_mapping_df = spark.createDataFrame(data, dicomExternalSchema) \
    .withColumn("base_path", get_base_path(F.col("path"))) \
    .drop("path")

inspection_result.select("path", "text").show(n=10, truncate=False)
inspection_result.select("path", "coordinates").show(n=10, truncate=False)
```

```python
entity_projection_columns = [
    "path",
    "entity.result AS detected_text",
    "entity.metadata['entity'] AS entity_type",
    "entity.begin AS begin",
    "entity.end AS end",
]

inspection_result.selectExpr("path", "explode(merged_ner_chunk) AS entity") \
    .selectExpr(*entity_projection_columns) \
    .show(n=100, truncate=False)
```

## Bad Spark API Formatting

Do not wrap Spark API chains in parentheses. Do not split Spark display calls across multiple argument lines.

```text
external_mapping_df = (
    spark.createDataFrame(data, dicomExternalSchema)
    .withColumn("base_path", get_base_path(F.col("path")))
    .drop("path")
)
```

```text
inspection_result.select(
    "path",
    "text",
).show(
    n=10,
    truncate=False,
)

inspection_result.selectExpr(
    "path",
    "explode(merged_ner_chunk) AS entity",
).selectExpr(
    "path",
    "entity.result AS detected_text",
    "entity.metadata['entity'] AS entity_type",
    "entity.begin AS begin",
    "entity.end AS end",
).show(
    n=100,
    truncate=False,
)
```

## Good File I/O Formatting

Keep file I/O calls on one line.

```python
with open(strategy_file_path, "w", encoding="utf-8", newline="") as file:
    file.write(csv_clean_tag_data)
```

## Bad File I/O Formatting

Do not split `open(...)` arguments across lines.

```text
with open(
    strategy_file_path,
    "w",
    encoding="utf-8",
    newline="",
) as file:
    file.write(csv_clean_tag_data)
```

## Good Function Formatting

Function definitions and calls may span multiple lines when the signature or argument list is long. Use standard Python indentation and spacing.

```python
def dicom_pipeline_builder(
    spark_session,
    config,
    intermediate_result,
    pretrained_pipeline,
    dicom_metadata_deid=None,
):
    return PipelineModel(stages=final_stages)


pipeline = dicom_pipeline_builder(
    spark_session=spark,
    config=config,
    intermediate_result=False,
    pretrained_pipeline=deid_pipeline,
    dicom_metadata_deid=None,
)
```

Keep simple exception statements on one line.

```python
raise ValueError(f"Unsupported OCR engine: {config['ocr_engine']!r}.")
```

## Good Output Block Formatting

When an answer includes a full notebook flow, output it in labeled blocks ordered as imports, helpers, config, strategy/mapping setup, pipeline steps summary, pipeline, data loading and prompts, run, display, and save. Keep the executable pipeline block contiguous. Strategy CSVs, group strategy CSVs, and `replaceWithMapping` external mapping DataFrames must be created before the pipeline block. Give the pipeline steps summary as plain numbered text immediately before the pipeline code block, not as a code comment.

```python
# Imports
from sparknlp.pretrained import PretrainedPipeline
```

```python
# Config
config = {
    "ocr_engine": "VLM",
    "scale": 0.75,
    "frame_sampling": 5,
}
```

```python
# Strategy / Mapping Setup
strategy_file_path = "dicom_metadata_deidentification_strategy.csv"
with open(strategy_file_path, "w", encoding="utf-8", newline="") as file:
    file.write(csv_strategy_data)
```

Pipeline steps:

1. `DicomMetadataDeidentifier` applies the tag strategy file to `path` and outputs `dicom_metadata_cleaned`.

```python
# Pipeline
dicom_deidentifier = DicomMetadataDeidentifier() \
    .setInputCols(["path"]) \
    .setOutputCol("dicom_metadata_cleaned") \
    .setStrategyFile(strategy_file_path)

pipeline = PipelineModel(stages=[
    dicom_deidentifier,
])
```

```python
# Data Loading And Prompts
pdf_path = "/path/to/pdf/files"
pdf_df = spark.read.format("binaryFile").load(pdf_path)
```

```python
# Run
result = pipeline.transform(pdf_df).cache()
```

## Good Named Value Formatting

Define long prompts, label lists, and config values before the stage or helper call.

```python
clinical_pipeline_name = "clinical_deidentification_docwise_benchmark_medium"
vlm_prompt = "Detect and recognize text in the image, and output the text coordinates in a formatted manner."
dicom_with_prompt_df = dicom_df.withColumn("caption", lit(vlm_prompt))
```

```python
phi_labels = [
    "DOCTOR",
    "PATIENT",
    "AGE",
    "DATE",
    "HOSPITAL",
    "CITY",
    "STREET",
    "STATE",
    "COUNTRY",
    "PHONE",
    "IDNUM",
    "EMAIL",
    "ZIP",
    "ORGANIZATION",
    "PROFESSION",
    "USERNAME",
]

zero_shot_models = [
    {
        "name": "zeroshot_ner_deid_subentity_merged_medium",
        "labels": phi_labels,
        "output_col": "phi_ner",
        "chunk_col": "phi_chunk",
    }
]

config = {
    "zero_shot_models": zero_shot_models,
    "scale": 0.75,
    "frame_sampling": 5,
}
```

## Good Pipeline List Formatting

`Pipeline(...)` and `PipelineModel(...)` may use multi-line stage lists when the stage list is long.

```python
pipeline = PipelineModel(stages=[
    dicom_to_image,
    caption_assembler,
    image_assembler,
    vlm_ocr,
    coordinate_extract,
    nlp_pipeline,
    position_finder,
    draw_regions,
])
```

```python
nlp_pipeline = Pipeline(stages=[
    document_assembler,
    sentence_detector,
    tokenizer,
    zero_shot_ner,
    ner_converter,
    chunk_merger,
])
```

## Bad Named Value Formatting

Do not split named string values with parenthesized continuation.

```text
clinical_pipeline_name = (
    "clinical_deidentification_docwise_benchmark_medium"
)
```

```text
vlm_prompt = (
    "Detect and recognize text in the image, and output the "
    "text coordinates in a formatted manner."
)
```
