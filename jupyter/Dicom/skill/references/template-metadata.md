# Metadata Templates

## metadata_inspection

```python
dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)

metadata = DicomToMetadata() \
    .setInputCol("content") \
    .setOutputCol("metadata_original") \
    .setKeepInput(False) \
    .setExtractTagForNer(False)

pipeline = PipelineModel(stages=[metadata])

result = pipeline.transform(dicom_df)

def build_metadata_df(dataframe, metadata_col="metadata_original"):
    import json
    import pandas as pd

    collect_result = []

    for row in dataframe.select("path", metadata_col).toLocalIterator():
        data = row.asDict()
        metadata = json.loads(data[metadata_col])

        for tag in metadata.keys():
            value = metadata[tag].get("value")
            vr = metadata[tag].get("vr")
            collect_result.append([data["path"], tag, vr, value])

    columns = ["Path", "Tag", "VR", "Value"]
    metadata_result_df = pd.DataFrame(collect_result, columns=columns)
    return metadata_result_df

metadata_original_df = build_metadata_df(result, metadata_col="metadata_original")
metadata_original_df.head()
```

## metadata_deid

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

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)

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

result = pipeline.transform(dicom_df)
display_dicom(df=result, fields="dicom_metadata_cleaned", limit=1, width=300)
```

Metadata comparison utility:

```python
def build_metadata_comparison_df(dataframe, original_col="metadata_original", cleaned_col="metadata_cleaned"):
    import json
    import pandas as pd

    collect_result = []

    for row in dataframe.select("path", original_col, cleaned_col).toLocalIterator():
        data = row.asDict()
        metadata_original = json.loads(data[original_col])
        metadata_cleaned = json.loads(data[cleaned_col])

        for tag in metadata_original.keys():
            original_value = metadata_original[tag]["value"]
            cleaned_value = "DELETED" if tag not in metadata_cleaned else metadata_cleaned[tag]["value"]
            value_changed = False if original_value == cleaned_value else True
            value_deleted = True if original_value != "DELETED" and cleaned_value == "DELETED" else False
            collect_result.append([tag, metadata_original[tag]["vr"], original_value, cleaned_value, value_changed, value_deleted])

    columns = ["Tag", "VR", "Original_Value", "Cleaned_Value", "Is_Changed", "Is_Deleted"]
    metadata_result_df = pd.DataFrame(collect_result, columns=columns)
    return metadata_result_df

metadata_result_df = build_metadata_comparison_df(result)
metadata_result_df.head()

def save_dicom_to_disk(dataframe, dicom_col="dicom_metadata_cleaned", output_dir="/tmp/dicom_deid"):
    from pathlib import Path

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    saved_paths = []

    for row in dataframe.select("path", dicom_col).toLocalIterator():
        base_file_name = Path(row["path"]).name
        target_path = output_path / base_file_name
        dicom_bytes = row[dicom_col]
        if isinstance(dicom_bytes, bytearray):
            dicom_bytes = bytes(dicom_bytes)
        with open(target_path, "wb") as f:
            f.write(dicom_bytes)
        saved_paths.append(str(target_path))

    return saved_paths

saved_paths = save_dicom_to_disk(result, dicom_col="dicom_metadata_cleaned", output_dir="/tmp/dicom_deid")
saved_paths[:5]
```

Optional group strategy:

```python
csv_group_strategy_data = dedent("""\
Tags,VR,Name,Status,Action,Option
"(0020,)",GROUP,None,,delete,
"(0040,)",GROUP,None,,delete,
""")

group_strategy_file_path = "dicom_metadata_group_strategy.csv"

with open(group_strategy_file_path, "w", encoding="utf-8", newline="") as file:
    file.write(csv_group_strategy_data)

print(f"Group strategy file saved to: {group_strategy_file_path}")

dicom_deidentifier = dicom_deidentifier.setGroupStrategyFile(group_strategy_file_path)
```

## metadata_clean_tag_medical_ner_legacy

Use only if the user explicitly asks for a legacy `MedicalNerModel` metadata cleanTag pipeline. The primary `metadata_clean_tag_ner` route lives in `template-zero-shot.md` and must use `build_stacked_zero_shot_metadata_pipeline(input_text="tag_text")`.
