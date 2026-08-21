# Pretrained Baseline Template

## pretrained_dicom_baseline

Use this as the first example when the user asks for the easiest pretrained Spark OCR DICOM baseline.

Swap `pipeline_name` with another Spark OCR pretrained DICOM pipeline from `models.yaml` when the user wants minimal, full anonymization, or pseudonym workflows.

```python
from sparkocr.pretrained import PretrainedPipeline

pipeline_name = "dicom_deid_generic_augmented_minimal"
pipeline = PretrainedPipeline(pipeline_name, "en", "clinical/ocr")

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)
result = pipeline.transform(dicom_df).cache()
display_dicom(df=result, fields="dicom", limit=1, width=300)

saved_paths = save_dicom_to_disk(result, dicom_col="dicom", output_dir="/tmp/dicom_deid")
saved_paths[:5]
```
