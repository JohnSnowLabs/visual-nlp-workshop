# Common Zero-Shot Builder Template

Use this file for OCR-text zero-shot PHI detection across DICOM pixel, DICOM encapsulated PDF, standalone image, and standalone PDF workflows.

Do not put DICOM metadata, DICOM tag strategy, DICOM drawing, image drawing, PDF reconstruction, display, or save code here. Keep those in the selected input-specific template.

## common_zero_shot_config

Read `models.yaml` `zero_shot_ner_models.options` before changing model names or labels.

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

zero_shot_models = [
    {
        "name": "zeroshot_ner_deid_subentity_merged_medium",
        "labels": subentity_merged_medium_labels,
        "output_col": "subentity_merged_medium_ner",
        "chunk_col": "subentity_merged_medium_chunk",
    }
]

config = {"zero_shot_models": zero_shot_models, "nPredict": 1024}
```

## common_zero_shot_two_model_config

Use this when the user explicitly asks for two stacked zero-shot NER models.

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

generic_docwise_medium_labels = [
    "AGE",
    "CONTACT",
    "DATE",
    "ID",
    "LOCATION",
    "NAME",
    "PROFESSION",
]

zero_shot_models = [
    {
        "name": "zeroshot_ner_deid_subentity_merged_medium",
        "labels": subentity_merged_medium_labels,
        "output_col": "subentity_merged_medium_ner",
        "chunk_col": "subentity_merged_medium_chunk",
    },
    {
        "name": "zeroshot_ner_deid_generic_docwise_medium",
        "labels": generic_docwise_medium_labels,
        "output_col": "generic_docwise_medium_ner",
        "chunk_col": "generic_docwise_medium_chunk",
    },
]

config = {"zero_shot_models": zero_shot_models, "nPredict": 1024}
```

## common_zero_shot_ner_builder

Use this builder after OCR has produced a plain text column. The default `input_text` is `text`, and the merged PHI chunk output is `merged_ner_chunk`.

```python
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

Use `common_zero_shot_position_finder` from `common/visual_stage.md` after this builder when OCR produced a `positions` page matrix.
