# Zero-Shot NER Models

Use this file when the user asks for zero-shot PHI detection, model/entity coverage, stacked NER models, or configurable NER for pixel or metadata de-identification.

The model-card links in this file are external John Snow Labs sources. When presenting model-card information to users, say it came from external JSL model cards or docs and that external information can change or may not be 100% correct for every installed version.

## Table Of Contents

- Model selection rules
- Stage usage pattern
- Stacking pattern
- Pixel PHI usage
- Metadata cleanTag usage
- Model catalog location

## Model Selection Rules

- Use these models for both pixel OCR text and metadata free-text de-identification.
- Read `models.yaml` `zero_shot_ner_models.options` for model names, one-line load code, model-card links, and entity lists.
- Use stackable stage-level `PretrainedZeroShotNER` models, not `PretrainedPipeline`, when multiple zero-shot models must run together.
- Do not pass these model names to the DICOM pipeline builder as `clinical_pipeline_name`.
- Do not mix this zero-shot stack with the builder path. If the user asks for a custom zero-shot hybrid, build direct custom stages in an outer `PipelineModel` instead of using `dicom_pipeline_builder(...)`.
- If the user asks for the default builder, use `clinical_pipeline` from `models.yaml`, not this file.
- For `metadata_clean_tag_ner`, use `build_stacked_zero_shot_metadata_pipeline(input_text="tag_text")`.
- For pixel or encapsulated PDF de-identification without the DICOM pipeline builder, use `build_stacked_zero_shot_ner_pipeline(input_text="text", output_chunk="merged_ner_chunk")`.
- Default to `zeroshot_ner_deid_subentity_merged_medium` for balanced coverage unless the user asks for a specific model.
- Use `large` models when the user asks for higher recall or accuracy and has enough resources.
- Use `generic` models when broad PHI classes are enough.
- Use `subentity` models when the user wants specific entity labels such as `DOCTOR`, `PATIENT`, `CITY`, `ZIP`, or `MEDICALRECORD`.
- Use `nonMedical` models when the text is not clinical prose or the user explicitly asks for non-medical coverage.
- Keep model labels as separate variables before stage definitions.
- Keep every `.pretrained(...)` call on one line.

## Stage Usage Pattern

Model cards use this general pattern. Start with a copy-ready model and swap the model name/labels from `models.yaml` when needed:

```python
subentity_merged_medium_labels = [
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

zero_shot_ner = PretrainedZeroShotNER().pretrained("zeroshot_ner_deid_subentity_merged_medium", "en", "clinical/models") \
    .setInputCols(["sentence", "token"]) \
    .setOutputCol("zero_shot_ner") \
    .setPredictionThreshold(0.5) \
    .setLabels(subentity_merged_medium_labels)
```

Always place `PretrainedZeroShotNER().pretrained(...)` on one line. Do not split `PretrainedZeroShotNER()` and `.pretrained(...)` across lines.

## Stacking Pattern

Use this pattern when the user wants to stack multiple zero-shot NER models for pixel OCR text or encapsulated PDF OCR text without the DICOM pipeline builder. Each model gets its own NER output and chunk output. Merge chunks into one downstream column.

```python
subentity_merged_medium_labels = [
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

config = {
    "zero_shot_models": [
        {
            "name": "zeroshot_ner_deid_subentity_merged_medium",
            "labels": subentity_merged_medium_labels,
            "output_col": "subentity_merged_medium_ner",
            "chunk_col": "subentity_merged_medium_chunk",
        }
    ]
}

def build_stacked_zero_shot_ner_pipeline(input_text="text", output_chunk="merged_ner_chunk"):
    document_assembler = DocumentAssembler() \
        .setInputCol(input_text) \
        .setOutputCol("document")

    sentence_detector = SentenceDetector() \
        .setInputCols(["document"]) \
        .setOutputCol("sentence")

    tokenizer = Tokenizer() \
        .setInputCols(["sentence"]) \
        .setOutputCol("token")

    stages = [document_assembler, sentence_detector, tokenizer]
    chunk_cols = []

    for model_settings in config["zero_shot_models"]:
        zero_shot_ner = PretrainedZeroShotNER().pretrained(model_settings["name"], "en", "clinical/models") \
            .setInputCols(["sentence", "token"]) \
            .setOutputCol(model_settings["output_col"]) \
            .setPredictionThreshold(0.5) \
            .setLabels(model_settings["labels"])

        ner_converter = NerConverterInternal() \
            .setInputCols(["sentence", "token", model_settings["output_col"]]) \
            .setOutputCol(model_settings["chunk_col"])

        stages.extend([zero_shot_ner, ner_converter])
        chunk_cols.append(model_settings["chunk_col"])

    chunk_merger = ChunkMergeApproach() \
        .setInputCols(chunk_cols) \
        .setOutputCol(output_chunk)

    stages.append(chunk_merger)

    nlp_pipeline = Pipeline(stages=stages)
    empty_data = spark.createDataFrame([[""]], [input_text])
    return nlp_pipeline.fit(empty_data)
```

## Pixel PHI Usage

For pixel OCR text, place the returned stacked NER `PipelineModel` inside the outer `PipelineModel` after OCR has produced `text`. For encapsulated PDF workflows, use `template-encapsulated-pdf.md`; do not adapt the DICOM pixel snippet below.

```python
pipeline = PipelineModel(stages=[
    dicom_to_image,
    caption_assembler,
    image_assembler,
    vlm_ocr,
    coordinate_extract,
    build_stacked_zero_shot_ner_pipeline(input_text="text", output_chunk="merged_ner_chunk"),
    position_finder,
    draw_regions
])
```

`PositionFinder` must consume only the merged chunk column:

```python
position_finder = PositionFinder() \
    .setInputCols(["merged_ner_chunk"]) \
    .setOutputCol("coordinates") \
    .setPageMatrixCol("positions") \
    .setIgnoreSchema(False) \
    .setOcrScaleFactor(1.1)
```

## Metadata cleanTag Usage

For metadata free-text de-identification, `DicomToMetadata` must extract `tag_text` and `tag_mapping`. The metadata NER subpipeline must start with `DocumentAssembler`, use `SentenceDetector` with the DICOM custom boundary, feed `DeIdentification`, and output `deid_documents`. Intermediate stage output names can vary as long as each downstream input is wired correctly. `DicomMetadataDeidentifier` must consume `deid_documents`.

Always use `build_stacked_zero_shot_metadata_pipeline(...)` for `metadata_clean_tag_ner`. Do not use `build_stacked_zero_shot_ner_pipeline(...)` for metadata cleanTag.

```python
def build_stacked_zero_shot_metadata_pipeline(input_text="tag_text"):
    document_assembler = DocumentAssembler() \
        .setInputCol(input_text) \
        .setOutputCol("t_document")

    sentence_detector = SentenceDetector() \
        .setInputCols(["t_document"]) \
        .setOutputCol("t_sentence") \
        .setCustomBounds(["<dicom>"]) \
        .setUseCustomBoundsOnly(True)

    tokenizer = Tokenizer() \
        .setInputCols(["t_sentence"]) \
        .setOutputCol("t_token")

    stages = [document_assembler, sentence_detector, tokenizer]
    chunk_cols = []

    for model_settings in config["zero_shot_models"]:
        zero_shot_ner = PretrainedZeroShotNER().pretrained(model_settings["name"], "en", "clinical/models") \
            .setInputCols(["t_sentence", "t_token"]) \
            .setOutputCol(model_settings["output_col"]) \
            .setPredictionThreshold(0.5) \
            .setLabels(model_settings["labels"])

        ner_converter = NerConverterInternal() \
            .setInputCols(["t_sentence", "t_token", model_settings["output_col"]]) \
            .setOutputCol(model_settings["chunk_col"])

        stages.extend([zero_shot_ner, ner_converter])
        chunk_cols.append(model_settings["chunk_col"])

    chunk_merger = ChunkMergeApproach() \
        .setInputCols(chunk_cols) \
        .setOutputCol("t_ner_chunk")

    deid_documents = DeIdentification() \
        .setInputCols(["t_sentence", "t_token", "t_ner_chunk"]) \
        .setOutputCol("deid_documents") \
        .setMode("deid")

    stages.extend([chunk_merger, deid_documents])

    nlp_pipeline = Pipeline(stages=stages)
    empty_data = spark.createDataFrame([[""]], [input_text])
    return nlp_pipeline.fit(empty_data)
```

Then use:

```python
pipeline = PipelineModel(stages=[
    dicom_to_metadata,
    build_stacked_zero_shot_metadata_pipeline(input_text="tag_text"),
    dicom_deidentifier,
    deid_metadata
])
```

The metadata de-identifier must include:

```python
from textwrap import dedent

csv_clean_tag_data = dedent("""\
Tags,VR,Name,Status,Action,Option
"(0008, 1030)",LO,Study Description,,cleanTag,deid
""")

strategy_file_path = "dicom_metadata_clean_tag_strategy.csv"

with open(strategy_file_path, "w", encoding="utf-8", newline="") as file:
    file.write(csv_clean_tag_data)

print(f"Strategy file saved to: {strategy_file_path}")

dicom_deidentifier = DicomMetadataDeidentifier() \
    .setInputCols(["path"]) \
    .setOutputCol("dicom_metadata_cleaned") \
    .setTagMappingCol("tag_mapping") \
    .setTagCleanedCol("deid_documents") \
    .setKeepInput(True) \
    .setStrategyFile(strategy_file_path)
```

## Model Catalog Location

The zero-shot model catalog lives in `models.yaml` under `zero_shot_ner_models.options`. Keep model names, one-line load code, model-card links, and entity lists there. Keep this file focused on usage patterns.
