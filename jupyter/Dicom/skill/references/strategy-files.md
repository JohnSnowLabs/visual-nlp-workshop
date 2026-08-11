# Strategy Files

Use strategy files with `DicomMetadataDeidentifier`.

## Default Response Behavior

When the user asks about `DicomMetadataDeidentifier`, `DicomMetadataDeIdentifier`, metadata de-identification, or strategy files, show the full action catalog before suggesting a starter CSV. Do not show only the actions used in the example.

Use this order:
1. Show the metadata action catalog.
2. Mention the group-level action catalog separately.
3. If the user provided `Tags`, `VR`, and `Name`, map `Action` and `Option` for them instead of asking them to choose action names when the mapping is clear.
4. Then give the starter strategy file that matches the request.

## Shape

```csv
Tags,VR,Name,Status,Action,Option
"(0010, 0010)",PN,Patient Name,,replaceWithLiteral,<REMOVED>
"(0010, 0020)",LO,Patient ID,,hashId,
"(0008, 1030)",LO,Study Description,,cleanTag,deid
```

When the user provides only `Tags`, `VR`, and `Name`, preserve those values exactly and infer `Action` and `Option` from the tag name, VR, and requested mode. User-provided tags can arrive as pasted text, messy notes, markdown tables, CSV/TSV, JSON, spreadsheets, file paths, DataFrame previews, notebook output, or bullet lists. Do not apply hard rules to the input format or require exact column names; normalize flexible inputs into internal `Tags`, `VR`, and `Name` rows first. Use the mapping policy in `template-strategy-file.md`. If any row is ambiguous, unparseable, missing required fields, or the model is unsure, ask a focused question before filling `Action` and `Option`; do not guess.

## Default CSV Creation Pattern

For `strategy_file_path`, `group_strategy_file_path`, `cleanTag`, and `replaceWithMapping`, always create the CSV with `textwrap.dedent`, save it to disk, and pass the path to the DICOM stage. This keeps generated examples easy to inspect, edit, and reuse.

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

Use this file-backed pattern first:

```python
dicom_deidentifier = DicomMetadataDeidentifier() \
    .setStrategyFile(strategy_file_path)
```

For normal tag strategy files, the in-memory alternate is:

```python
dicom_deidentifier = DicomMetadataDeidentifier() \
    ._set(strategyFileContent=csv_strategy_data)
```

## Metadata Action Catalog

Always show this full catalog when explaining metadata strategy files.

| Action | What it does | Option |
|---|---|---|
| `cleanTag` | De-identifies free-text metadata using NLP output. | `remove`, `deid`, or `mask` |
| `remove` | Keeps the tag but empties its value. | empty |
| `delete` | Deletes the tag. | empty |
| `hashId` | Deterministically hashes an identifier. | empty |
| `patientHashId` | Creates a patient-specific identifier hash. | empty |
| `replaceWithLiteral` | Replaces the tag value with the literal in `Option`. | replacement text |
| `replaceWithMapping` | Replaces the value using external mapping data. | `Nested` when nested mapping is needed |
| `replaceWithRandomName` | Replaces the value with a generated random person name. | empty |
| `shiftDateByFixedNbOfDays` | Shifts dates by a fixed number of days. | day count |
| `shiftDateByRandomNbOfDays` | Shifts dates by a random number of days. | empty or configured range if supplied by the user |
| `shiftTimeByRandom` | Randomly shifts time values. | empty |
| `shiftUnixTimeStampRandom` | Randomly shifts Unix timestamp values. | empty |
| `shiftAgeByRandom` | Randomly shifts age values. | empty |
| `capAgeAt99IfOver90` | Caps ages above 90 at 99. | empty |

Do not mention or include omitted legacy actions in user-facing responses or examples.

## Group-Level Strategy Actions

Group-level actions are not normal per-tag actions. Use a separate group strategy file when all tags in a DICOM group should be removed or deleted, including nested tags.

Supported group-level actions:
- `remove`: remove values for all tags in the group.
- `delete`: delete all tags in the group.

CSV shape:

```csv
Tags,VR,Name,Status,Action,Option
"(0020,)",GROUP,None,,delete,
"(0040,)",GROUP,None,,delete,
```

The group tag format is `"(GGGG,)"`, such as `"(0020,)"`.

Pass a group strategy file path:

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

dicom_deidentifier = DicomMetadataDeidentifier() \
    .setGroupStrategyFile(group_strategy_file_path)
```

Or pass in-memory group strategy CSV content:

```python
dicom_deidentifier = DicomMetadataDeidentifier() \
    ._set(groupStrategyFileContent=csv_group_strategy_data)
```

When a normal tag strategy and group strategy are both needed, configure both on the same `DicomMetadataDeidentifier`.

## Nested Rule

`replaceWithMapping` is the only action that uses explicit `Nested` in `Option`. Other actions are nested by default.

```csv
Tags,VR,Name,Status,Action,Option
"(0008, 0018)",UI,SOP Instance UID,,replaceWithMapping,Nested
```

## External Mapping Schema

For `replaceWithMapping`, always create the external mapping DataFrame with predefined `dicomExternalSchema` from `sparkocr.schemas`.

The metadata strategy CSV that uses `replaceWithMapping` must still be written to disk first with the default CSV creation pattern.

Schema meaning:
- `path`: path or filename used to identify the DICOM file.
- `external_mapping`: map of DICOM tags to replacement values and value types.

Mapping shape:

```text
path: String
external_mapping: Map[DICOM tag, (replacement value, value type)]
```

DICOM tags in external mapping must not include parentheses or commas. Use `00100020`, not `(0010,0020)`.

Replacement value types:
- `0`: integer.
- `1`: float.
- `2`: string.

The external mapping DataFrame must be joined to the DICOM DataFrame using path values that match exactly. If the DICOM DataFrame has full paths but the mapping DataFrame has filenames, the join will not work until both sides are normalized to the same value. In that case, derive `base_path` on both DataFrames and join by `base_path`.

```python
import os
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

@F.udf(returnType=StringType())
def get_base_path(path):
    return os.path.basename(path) if path else None
    
dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path).withColumn("base_path", get_base_path(F.col("path")))

data = [
    (
        "David_Douglas.dcm",
        {
            "00080016": ("1.1.111.1.1.1111111.1.111.111111111111", 2),
            "00080018": ("1.1.111.1.1.1111111.1.111.111111111111", 2),
            "00080020": ("20160729", 2),
            "00080021": ("20160729", 2),
            "00080022": ("20160729", 2),
            "00080023": ("20160729", 2),
            "00100010": ("JOHN^DOE", 2),
            "00100020": ("111111", 2),
            "0020000D": ("1.1.111.1.1.1111111.1.111.111111111111", 2),
            "0020000E": ("1.1.111.1.1.1111111.1.111.111111111111", 2),
            "0040A121": ("20160729", 2),
            "0040A122": ("103", 2),
        },
    )
]

external_mapping_df = spark.createDataFrame(data, dicomExternalSchema).withColumn("base_path", get_base_path(F.col("path"))).drop("path")
external_mapping_df.select(F.explode("external_mapping")).show(100, False)
dicom_df = dicom_df.join(external_mapping_df, "base_path", "left")
```

Use the default external mapping column unless the user explicitly asks for a different one:

```python
from textwrap import dedent

csv_strategy_data = dedent("""\
Tags,VR,Name,Status,Action,Option
"(0010, 0010)",PN,Patient Name,,replaceWithMapping,Nested
"(0010, 0020)",LO,Patient ID,,replaceWithMapping,Nested
""")

strategy_file_path = "dicom_metadata_replace_with_mapping_strategy.csv"

with open(strategy_file_path, "w", encoding="utf-8", newline="") as file:
    file.write(csv_strategy_data)

print(f"Strategy file saved to: {strategy_file_path}")

dicom_deidentifier = DicomMetadataDeidentifier() \
    .setInputCols(["path"]) \
    .setOutputCol("dicom_metadata_cleaned") \
    .setKeepInput(True) \
    .setStrategyFile(strategy_file_path) \
    .setExternalMappingCol("external_mapping")
```

## cleanTag

Use `cleanTag` for free-text fields such as study descriptions, protocol names, and comments.

The `cleanTag` strategy CSV must be written to disk first with the default CSV creation pattern.

Options:
- `remove`: remove detected PHI.
- `deid`: replace detected PHI with entity placeholders.
- `mask`: mask PHI characters.

`cleanTag` requires:
- `DicomToMetadata.setExtractTagForNer(True)`
- `tag_text`
- `tag_mapping`
- Healthcare NLP output `deid_documents`
- `DicomMetadataDeidentifier.setTagCleanedCol("deid_documents")`
