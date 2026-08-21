# Metadata Templates

## metadata_inspection

```python
metadata = DicomToMetadata() \
    .setInputCol("content") \
    .setOutputCol("metadata_original") \
    .setKeepInput(False) \
    .setExtractTagForNer(False)

pipeline = PipelineModel(stages=[metadata])

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)
result = pipeline.transform(dicom_df).cache()

metadata_original_df = build_metadata_df(result, metadata_col="metadata_original")
metadata_original_df.head()
```

## metadata_deid

Create `csv_strategy_data` and `strategy_file_path` first, using the `metadata_creation_pattern` code shape from `metadata-creation.md`:

```python
from textwrap import dedent

csv_strategy_data = dedent("""\
Tags,VR,Name,Status,Action,Option
"(0010, 0010)",PN,Patient Name,,replaceWithLiteral,<REMOVED>
"(0010, 0020)",LO,Patient ID,,hashId,
""")

strategy_file_path = "dicom_metadata_deidentification_strategy.csv"

with open(strategy_file_path, "w", encoding="utf-8", newline="") as file:
    file.write(csv_strategy_data)

print(f"Strategy file saved to: {strategy_file_path}")
```

Then build the pipeline:

```python
original_metadata = DicomToMetadata() \
    .setInputCol("content") \
    .setOutputCol("metadata_original") \
    .setKeepInput(False) \
    .setExtractTagForNer(False)

dicom_deidentifier = DicomMetadataDeidentifier() \
    .setInputCols(["path"]) \
    .setOutputCol("dicom_metadata_cleaned") \
    .setKeepInput(True) \
    .setRemovePrivateTags(False) \
    .setStrategyFile(strategy_file_path)

deid_metadata = DicomToMetadata() \
    .setInputCol("dicom_metadata_cleaned") \
    .setOutputCol("metadata_cleaned") \
    .setKeepInput(True) \
    .setExtractTagForNer(False)

pipeline = PipelineModel(stages=[original_metadata, dicom_deidentifier, deid_metadata])

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)
result = pipeline.transform(dicom_df).cache()
display_dicom(df=result, fields="dicom_metadata_cleaned", limit=1, width=300)
```

Metadata comparison:

```python
metadata_result_df = build_metadata_comparison_df(result)
metadata_result_df.head()

saved_paths = save_dicom_to_disk(result, dicom_col="dicom_metadata_cleaned", output_dir="/tmp/dicom_deid")
saved_paths[:5]
```

For optional group strategy examples, use `dicom/strategy-files.md` `Group-Level Strategy Actions`.

## metadata_clean_tag_medical_ner_legacy

Use only if the user explicitly asks for a legacy `MedicalNerModel` metadata cleanTag pipeline. The primary `metadata_clean_tag_ner` route lives in `template-zero-shot.md` and must use `build_stacked_zero_shot_metadata_pipeline(input_text="tag_text")`.
