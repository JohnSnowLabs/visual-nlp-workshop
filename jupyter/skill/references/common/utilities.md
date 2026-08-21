# Utilities

Keep utilities in common references, but keep their call sites input-specific.

Shared rules:

- Include save utilities only when the workflow produces a final output file.
- Preserve the source base filename when saving transformed outputs unless the user asks otherwise.
- Include only the DICOM, image, PDF, or metadata utility section that matches the selected route. Include the intermediate utility section only when the user explicitly asks for intermediate results.
- Keep scale-sensitive redaction settings in the pipeline template; display/save utilities should only consume the final output columns.
- Put DICOM save/display call sites in DICOM answers only.
- Put image save/display call sites in image answers only.
- Put PDF save/display call sites in PDF answers only.

## common_display

Use this section for reusable display helpers. Include only the subsection that matches the active input type and output column.

### dicom_display_utility

Use this direct call for final DICOM-producing workflows. Keep DICOM pixel/redaction scale handling inside the pipeline stages, especially `DicomDrawRegions.setScaleFactor(1 / config["scale"])`; this utility only displays the final DICOM bytes column.

```python
display_dicom(df=result, fields="dicom_metadata_cleaned", limit=1, width=300)
```

### image_display_utility

Use `display_images(...)` for a single image column. Use `display_images_horizontal(...)` when comparing the original image and the de-identified image side by side.

```python
display_images(df=result, field="image_with_regions", limit=5, width=600, show_meta=True)
display_images_horizontal(df=result, fields="image,image_with_regions", limit=5, width=700, show_meta=True)
```

### pdf_display_utility

Use this direct call for standalone PDF workflows that produce a final binary PDF column such as `pdf`.

```python
display_pdf(df=result, field="pdf", limit=5, width=700, show_meta=True)
```

### intermediate_display_utility

Use this only when the user explicitly asks for intermediate results before aggregation, DICOM drawing, PDF reconstruction, or final byte-only output stages remove intermediate columns.

```python
def display_intermediate_result(dataframe, columns, limit=10, truncate=False):
    dataframe.select(*columns).show(limit, truncate)
```

## common_save

Use this section for reusable save helpers. Include only the subsection that matches the active input type and final output column.

### dicom_save_utility

Use this for DICOM workflows that produce a final DICOM bytes column. Preserve `path` base filenames and pass the route-specific DICOM column at the call site.

```python
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
```

### image_save_utility

Use this for standalone image workflows that produce an image schema column such as `image_with_regions`. Preserve the source base filename when possible.

```python
def save_image_to_disk(
    dataframe,
    image_col="image_with_regions",
    output_dir="/tmp/image_deid",
    default_extension=".png",
):
    from pathlib import Path

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    saved_paths = []

    for row in dataframe.select("path", image_col).toLocalIterator():
        source_path = Path(row["path"])
        suffix = source_path.suffix or default_extension
        target_path = output_path / f"{source_path.stem}{suffix}"
        image_data = row[image_col]
        if hasattr(image_data, "asDict"):
            image_data = image_data.asDict(recursive=True)
        if isinstance(image_data, dict):
            image_data = image_data.get("data") or image_data.get("bytes") or image_data.get("content")
        if isinstance(image_data, bytearray):
            image_data = bytes(image_data)
        with open(target_path, "wb") as f:
            f.write(image_data)
        saved_paths.append(str(target_path))

    return saved_paths
```

### pdf_save_utility

Use this for standalone PDF workflows that produce a final binary PDF column such as `pdf`.

```python
def save_pdf_to_disk(dataframe, pdf_col="pdf", output_dir="/tmp/pdf_deid"):
    from pathlib import Path

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    saved_paths = []

    for row in dataframe.select("path", pdf_col).toLocalIterator():
        source_path = Path(row["path"])
        target_path = output_path / f"{source_path.stem}.pdf"
        pdf_bytes = row[pdf_col]
        if isinstance(pdf_bytes, bytearray):
            pdf_bytes = bytes(pdf_bytes)
        with open(target_path, "wb") as f:
            f.write(pdf_bytes)
        saved_paths.append(str(target_path))

    return saved_paths
```

## DICOM Metadata Utilities

Use these helpers for DICOM metadata inspection and metadata de-identification examples.

- Use this input pattern in generated DICOM examples after the pipeline block and before `pipeline.transform(...)`:

```python
dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)
```

- Use `display_dicom(df=result, fields="<dicom_col>", limit=1, width=300)` from `common_display` for DICOM pipeline validation.
- Use `display_images` only when the user explicitly wants to inspect image content.
- To save DICOM bytes to disk, use `save_dicom_to_disk(...)` from `common_save` with the final DICOM bytes column.
- Use `build_metadata_df(result, metadata_col="metadata_original")` for single metadata column inspection.
- Use `build_metadata_comparison_df(result)` after metadata de-identification pipelines that output `metadata_original` and `metadata_cleaned`.
- Outside single metadata inspection and intentional intermediate inspection pipelines, prefer `display_dicom(...)` over post-transform `result.select(...)` examples.

```python
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
            collect_result.append(
                [
                    tag,
                    metadata_original[tag]["vr"],
                    original_value,
                    cleaned_value,
                    value_changed,
                    value_deleted,
                ]
            )

    columns = ["Tag", "VR", "Original_Value", "Cleaned_Value", "Is_Changed", "Is_Deleted"]
    metadata_result_df = pd.DataFrame(collect_result, columns=columns)
    return metadata_result_df
```
