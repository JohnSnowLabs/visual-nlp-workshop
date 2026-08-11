# Metadata Strategy File Template

Use this file when routing selects `metadata_strategy_file`, or when the user asks to create, design, review, or explain a DICOM metadata strategy file.

Always show the action catalog first, then generate the CSV file that matches the user's goal. Do not show omitted legacy actions.

## metadata_strategy_file

If the user provides only `Tags`, `VR`, and `Name`, complete the strategy file on their behalf by mapping each row to `Action` and `Option`. Preserve the user's tag, VR, and name exactly. If the mapping is clear, do not ask the user to choose Spark OCR action names. If any row is ambiguous or the model is unsure, ask a focused question before filling `Action` and `Option`; do not guess.

User tag input can arrive in any reasonable format: pasted text, messy notes, markdown tables, CSV/TSV, JSON, spreadsheets, file paths, DataFrame previews, notebook output, or bullet lists. Do not require exact column names or clean formatting. Accept variants such as `Tag`, `Tags`, `DICOM Tag`, `VR`, `Value Representation`, `Name`, `Keyword`, `Description`, or similar labels. First normalize the input into internal rows with `Tags`, `VR`, and `Name`; only the final generated strategy file must use the canonical `Tags,VR,Name,Status,Action,Option` CSV shape. If the input cannot be parsed confidently, ask the user for the missing or unclear fields instead of rejecting the format or guessing.

Use this action selection table:

| User goal | Action | Option |
|---|---|---|
| Empty a tag but keep the tag | `remove` | empty |
| Delete a tag entirely | `delete` | empty |
| Deterministically hash an identifier | `hashId` | empty |
| Hash an identifier using patient-level logic | `patientHashId` | empty |
| Replace with a fixed visible value | `replaceWithLiteral` | replacement text |
| Replace from an external mapping DataFrame | `replaceWithMapping` | `Nested` |
| Replace a person name with a random name | `replaceWithRandomName` | empty |
| Clean free-text metadata with NLP | `cleanTag` | `remove`, `deid`, or `mask` |
| Shift dates by a known number of days | `shiftDateByFixedNbOfDays` | day count |
| Shift dates randomly | `shiftDateByRandomNbOfDays` | empty |
| Shift times randomly | `shiftTimeByRandom` | empty |
| Shift Unix timestamps randomly | `shiftUnixTimeStampRandom` | empty |
| Shift ages randomly | `shiftAgeByRandom` | empty |
| Cap ages above 90 | `capAgeAt99IfOver90` | empty |

Use this default mapping policy for user-provided `Tags, VR, Name` rows:

| Tag/name pattern | Default action | Default option |
|---|---|---|
| Patient name or person name | `replaceWithLiteral` | `<REMOVED>` |
| Physician, doctor, operator, performer, or referring provider name | `replaceWithRandomName` | empty |
| Patient ID, MRN, accession, account, admission, issuer, serial, device ID, or other identifier | `hashId` | empty |
| Study/series/SOP UID or instance UID | `hashId` | empty |
| Birth date | `remove` | empty |
| Other date fields | `shiftDateByRandomNbOfDays` | empty |
| Time fields | `shiftTimeByRandom` | empty |
| Age fields | `capAgeAt99IfOver90` | empty |
| Institution, organization, department, station, address, phone, email, or contact fields | `remove` | empty |
| Description, comments, protocol, reason, history, diagnosis, note, or other free-text fields | `cleanTag` | `deid` |
| Private tags or user-requested aggressive metadata cleanup | `delete` | empty |

When the user asks for pseudonymization or provides external replacement values, override the default mapping and use `replaceWithMapping,Nested` for mapped tags.

If a tag does not match the policy clearly, ask what should happen to that tag. Good question shape: `For (xxxx, yyyy) <Name>, should I remove the value, delete the tag, hash it, replace it with a literal, clean it with NER, or map it from external values?`

Use this copy-ready helper when the user provides tag rows and wants the agent to map actions:

```python
from textwrap import dedent
from io import StringIO
import csv

user_tags = [
    {"Tags": "(0010, 0010)", "VR": "PN", "Name": "Patient Name"},
    {"Tags": "(0010, 0020)", "VR": "LO", "Name": "Patient ID"},
    {"Tags": "(0008, 0020)", "VR": "DA", "Name": "Study Date"},
    {"Tags": "(0008, 1030)", "VR": "LO", "Name": "Study Description"},
]

def infer_metadata_action(tag, vr, name, mode="phi_only"):
    name_lower = name.lower()
    if mode == "pseudonym":
        return "replaceWithMapping", "Nested"
    if "private" in name_lower or mode == "aggressive":
        return "delete", ""
    if "description" in name_lower or "comment" in name_lower or "protocol" in name_lower or "reason" in name_lower or "history" in name_lower or "diagnosis" in name_lower or "note" in name_lower:
        return "cleanTag", "deid"
    if "birth" in name_lower and "date" in name_lower:
        return "remove", ""
    if vr == "DA" or "date" in name_lower:
        return "shiftDateByRandomNbOfDays", ""
    if vr == "TM" or "time" in name_lower:
        return "shiftTimeByRandom", ""
    if vr == "AS" or "age" in name_lower:
        return "capAgeAt99IfOver90", ""
    if "physician" in name_lower or "doctor" in name_lower or "operator" in name_lower or "performer" in name_lower or "referring" in name_lower:
        return "replaceWithRandomName", ""
    if vr == "PN" or "patient name" in name_lower or "person name" in name_lower:
        return "replaceWithLiteral", "<REMOVED>"
    if "uid" in name_lower or "id" in name_lower or "identifier" in name_lower or "accession" in name_lower or "account" in name_lower or "admission" in name_lower or "issuer" in name_lower or "serial" in name_lower:
        return "hashId", ""
    if "institution" in name_lower or "organization" in name_lower or "department" in name_lower or "station" in name_lower or "address" in name_lower or "phone" in name_lower or "email" in name_lower or "contact" in name_lower:
        return "remove", ""
    return None, None

csv_buffer = StringIO()
csv_buffer.write(dedent("""\
Tags,VR,Name,Status,Action,Option
"""))
csv_writer = csv.writer(csv_buffer)

for item in user_tags:
    action, option = infer_metadata_action(item["Tags"], item["VR"], item["Name"], mode="phi_only")
    if action is None:
        raise ValueError(f"Ambiguous metadata strategy action for {item['Tags']} {item['Name']}. Ask the user before guessing.")
    csv_writer.writerow([item["Tags"], item["VR"], item["Name"], "", action, option])

csv_strategy_data = csv_buffer.getvalue()
strategy_file_path = "dicom_metadata_deidentification_strategy.csv"

with open(strategy_file_path, "w", encoding="utf-8", newline="") as file:
    file.write(csv_strategy_data)

print(f"Strategy file saved to: {strategy_file_path}")
```

Use this starter template when the user wants common PHI metadata cleanup:

```python
from textwrap import dedent

csv_strategy_data = dedent("""\
Tags,VR,Name,Status,Action,Option
"(0010, 0010)",PN,Patient Name,,replaceWithLiteral,<REMOVED>
"(0010, 0020)",LO,Patient ID,,hashId,
"(0010, 0030)",DA,Patient Birth Date,,remove,
"(0010, 0040)",CS,Patient Sex,,remove,
"(0008, 0090)",PN,Referring Physician Name,,replaceWithRandomName,
"(0008, 0080)",LO,Institution Name,,delete,
"(0008, 0020)",DA,Study Date,,shiftDateByRandomNbOfDays,
"(0008, 0030)",TM,Study Time,,shiftTimeByRandom,
"(0010, 1010)",AS,Patient Age,,capAgeAt99IfOver90,
"(0011, 1010)",LO,Private Unix Timestamp,,shiftUnixTimeStampRandom,
"(0011, 1020)",AS,Private Age Value,,shiftAgeByRandom,
""")

strategy_file_path = "dicom_metadata_deidentification_strategy.csv"

with open(strategy_file_path, "w", encoding="utf-8", newline="") as file:
    file.write(csv_strategy_data)

print(f"Strategy file saved to: {strategy_file_path}")
```

Use this when the user wants free-text metadata cleaned by NER:

```python
from textwrap import dedent

csv_clean_tag_data = dedent("""\
Tags,VR,Name,Status,Action,Option
"(0008, 1030)",LO,Study Description,,cleanTag,deid
"(0018, 1030)",LO,Protocol Name,,cleanTag,deid
"(0040, 4000)",LT,Comments on the Performed Procedure Step,,cleanTag,deid
""")

strategy_file_path = "dicom_metadata_clean_tag_strategy.csv"

with open(strategy_file_path, "w", encoding="utf-8", newline="") as file:
    file.write(csv_clean_tag_data)

print(f"Strategy file saved to: {strategy_file_path}")
```

Use this when the user wants external pseudonym values from a mapping table. For `replaceWithMapping`, the strategy file uses `(group, element)` tags, but the external mapping DataFrame uses compact tag IDs without parentheses or commas.

```python
from textwrap import dedent

csv_mapping_strategy_data = dedent("""\
Tags,VR,Name,Status,Action,Option
"(0010, 0010)",PN,Patient Name,,replaceWithMapping,Nested
"(0010, 0020)",LO,Patient ID,,replaceWithMapping,Nested
"(0008, 0018)",UI,SOP Instance UID,,replaceWithMapping,Nested
""")

strategy_file_path = "dicom_metadata_replace_with_mapping_strategy.csv"

with open(strategy_file_path, "w", encoding="utf-8", newline="") as file:
    file.write(csv_mapping_strategy_data)

print(f"Strategy file saved to: {strategy_file_path}")
```

External mapping DataFrame pattern. The path values must match the DICOM DataFrame path values for the join to work. If one side has full paths and the other has filenames, normalize both to `base_path` before joining.

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
            "00100010": ("JOHN^DOE", 2),
            "00100020": ("111111", 2),
            "00080018": ("1.1.111.1.1.1111111.1.111.111111111111", 2),
        },
    )
]

external_mapping_df = spark.createDataFrame(data, dicomExternalSchema).withColumn("base_path", get_base_path(F.col("path"))).drop("path")
dicom_df = dicom_df.join(external_mapping_df, "base_path", "left")
```

Use this when the user wants to remove or delete whole DICOM groups:

```python
from textwrap import dedent

csv_group_strategy_data = dedent("""\
Tags,VR,Name,Status,Action,Option
"(0020,)",GROUP,None,,delete,
"(0040,)",GROUP,None,,delete,
""")

group_strategy_file_path = "dicom_metadata_group_strategy.csv"

with open(group_strategy_file_path, "w", encoding="utf-8", newline="") as file:
    file.write(csv_group_strategy_data)

print(f"Group strategy file saved to: {group_strategy_file_path}")
```

Attach the files to `DicomMetadataDeidentifier` like this:

```python
dicom_deidentifier = DicomMetadataDeidentifier() \
    .setInputCols(["path"]) \
    .setOutputCol("dicom_metadata_cleaned") \
    .setKeepInput(True) \
    .setStrategyFile(strategy_file_path)
```

If group strategy is also needed:

```python
dicom_deidentifier = dicom_deidentifier.setGroupStrategyFile(group_strategy_file_path)
```

If external mapping is used:

```python
dicom_deidentifier = dicom_deidentifier.setExternalMappingCol("external_mapping")
```

For normal tag strategy files and group strategy files, only show in-memory `._set(...)` as an alternate after the disk-backed CSV example:

```python
dicom_deidentifier = dicom_deidentifier._set(strategyFileContent=csv_strategy_data)
dicom_deidentifier = dicom_deidentifier._set(groupStrategyFileContent=csv_group_strategy_data)
```
