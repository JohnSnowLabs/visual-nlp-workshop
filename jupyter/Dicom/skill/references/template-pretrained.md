# Pretrained Baseline Template

## pretrained_dicom_baseline

Use this as the first example when the user asks for the easiest pretrained Spark OCR DICOM baseline.

Swap `pipeline_name` with another Spark OCR pretrained DICOM pipeline from `models.yaml` when the user wants minimal, full anonymization, or pseudonym workflows.

```python
from sparkocr.pretrained import PretrainedPipeline

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)

pipeline_name = "dicom_deid_generic_augmented_minimal"
pipeline = PretrainedPipeline(pipeline_name, "en", "clinical/ocr")

result = pipeline.transform(dicom_df)
display_dicom(df=result, fields="dicom", limit=1, width=300)

def save_dicom_to_disk(dataframe, dicom_col="dicom", output_dir="/tmp/dicom_deid"):
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

saved_paths = save_dicom_to_disk(result, dicom_col="dicom", output_dir="/tmp/dicom_deid")
saved_paths[:5]
```
