# Metadata Strategy CSV Creation

Canonical pattern for creating and saving a DICOM metadata strategy CSV before it is consumed by `DicomMetadataDeidentifier`, a pipeline builder, or a pipeline. Every route that writes `strategy_file_path`, `group_strategy_file_path`, a `cleanTag` strategy CSV, or a `replaceWithMapping` external mapping DataFrame must follow this pattern: build the CSV content with `textwrap.dedent`, write it to disk, and print a confirmation, all before defining any pipeline or pipeline builder call that consumes the file.

## metadata_creation_pattern

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

Keep this statement order: define the CSV content variable first, then the file path variable, then write the file, then print the confirmation. Swap only the CSV row content and the two variable values to match the selected route or user request; do not change the statement order and do not drop the print confirmation.

Apply the same shape for a group strategy file (`csv_group_strategy_data` / `group_strategy_file_path`) and for a `cleanTag` strategy file — only the variable names, file name, and CSV rows change.
