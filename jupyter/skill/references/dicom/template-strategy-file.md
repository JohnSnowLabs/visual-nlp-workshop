# Metadata Strategy File Template

Use this file when routing selects `metadata_strategy_file`, or when the user asks to create, design, review, or explain a DICOM metadata strategy file.

This file is self-contained for the `metadata_strategy_file` route. It does not read, and must not be made to depend on, `dicom/strategy-files.md` or `dicom/metadata-creation.md` — those are used only by `metadata_deid` and `metadata_clean_tag_ner`. Keep every table, policy, and CSV-creation example this route needs defined inline in this file, so edits here never change the behavior of the other metadata routes, and edits to the other metadata routes' files never change this one.

Always show the action catalog first, then generate the CSV file that matches the user's goal. Do not show omitted legacy actions.

## metadata_strategy_file

### Action → Valid VR Compatibility

This is the hard constraint, taken from the `DicomMetadataDeidentifier` action dispatch table. An `Action` only works on the VRs listed for it; assigning an action to a tag whose VR is not listed fails at runtime. Never emit a strategy row whose `Action` is not valid for that row's `VR`. If a tag's VR is unknown, do not guess an action that depends on VR — ask, or use a VR-agnostic action.

| Action | Valid VRs |
|---|---|
| `replaceWithLiteral` | Any VR |
| `delete` | Any VR |
| `remove` | Any VR |
| `ensureTagExists` | Any VR |
| `replaceWithRandomName` | `PN`, `LO` |
| `cleanTag` | `PN`, `UI`, `SH`, `CS`, `LO`, `LT`, `ST`, `IS`, `TM`, `AE`, `US`, `AS`, `DS`, `OB`, `DT`, `OW`, `UT` |
| `hashId` | `UI`, `LO`, `SH` |
| `patientHashId` | `LO` |
| `shiftDateByFixedNbOfDays` | `DA`, `DT` |
| `shiftDateByRandomNbOfDays` | `DA`, `DT` |
| `shiftTimeByRandom` | `TM` |
| `shiftUnixTimeStampRandom` | `SL`, `FD` |
| `shiftAgeByRandom` | `AS` |
| `capAgeAt99IfOver90` | `AS` |
| `replaceWithMapping` | Any VR (external mapping value replaces the tag directly; not VR-dispatched) |

### Action Selection Table

| User goal | Action | Option |
|---|---|---|
| Empty a tag but keep the tag | `remove` | empty |
| Delete a tag entirely | `delete` | empty |
| Guarantee a tag exists (add it empty if missing) | `ensureTagExists` | empty |
| Deterministically hash an identifier | `hashId` | empty |
| Hash an identifier using patient-level logic | `patientHashId` | empty |
| Replace with a fixed visible value | `replaceWithLiteral` | replacement text |
| Replace from an external mapping DataFrame | `replaceWithMapping` | `Nested` |
| Replace a person name with a random name | `replaceWithRandomName` | empty, or `coherent` to keep the same original name mapped to the same random name |
| Clean free-text metadata with NLP | `cleanTag` | `remove`, `deid`, or `mask` |
| Shift dates by a known number of days | `shiftDateByFixedNbOfDays` | signed integer (e.g. `5` or `-5`); positive shifts forward, negative shifts backward. If `Option` is missing or not a valid integer, this action silently behaves like `shiftDateByRandomNbOfDays` instead — always supply a valid signed integer when a fixed shift is actually required. |
| Shift dates randomly | `shiftDateByRandomNbOfDays` | empty |
| Shift times randomly | `shiftTimeByRandom` | empty |
| Shift Unix timestamps randomly | `shiftUnixTimeStampRandom` | empty |
| Shift ages randomly | `shiftAgeByRandom` | empty |
| Cap ages above 90 | `capAgeAt99IfOver90` | empty |

### cleanTag Option Semantics

`cleanTag` operates on NLP-masked text that already contains entity placeholders such as `<ID>` and `<DATE>`. Its `Option` controls what happens to those placeholders, not the original PHI text:

- `remove`: delete the placeholder entirely, closing up the surrounding text.
- `deid`: keep the placeholder unchanged.
- `mask`: replace the placeholder with asterisks, one per character of the entity label (excluding the angle brackets) — e.g. `<ID>` (2 chars) becomes `**`, `<DATE>` (4 chars) becomes `****`.

Example, starting from the masked text `Patient : <ID> Was asked to perform test on <DATE>`:

| Option | Output |
|---|---|
| `remove` | `Patient : Was asked to perform test on` |
| `deid` | `Patient : <ID> Was asked to perform test on <DATE>` |
| `mask` | `Patient : ** Was asked to perform test on ****` |

Valid options: `remove`, `deid`, `mask`. If `Option` is empty, `cleanTag` defaults to `deid`. Reject or ask about any other value.

### replaceWithRandomName Option Semantics

- empty: each occurrence gets an independently generated random name, even for the same original value.
- `coherent`: the same original name always maps to the same random replacement name, so repeated occurrences of one person stay consistent.

Default to empty unless the user asks for consistent/coherent name replacement across rows or documents.

By contrast, `hashId` and `patientHashId` are coherent by default — deterministic hashing always maps the same original value to the same output, with no `coherent` option needed.

### Default Mapping Policy

Use this default mapping for user-provided `Tags, VR, Name` rows. Every default here is a valid `Action`/`VR` pair per the compatibility table above.

| Tag/name pattern | Default action | Default option |
|---|---|---|
| Patient name or person name (`PN`) | `replaceWithLiteral` | `<REMOVED>` |
| Physician, doctor, operator, performer, or referring provider name (`PN`/`LO`) | `replaceWithRandomName` | empty |
| Patient ID, MRN, accession, account, admission, issuer, serial, device ID, or other identifier (`UI`/`LO`/`SH`) | `hashId` | empty |
| Study/series/SOP UID or instance UID (`UI`) | `hashId` | empty |
| Birth date (`DA`) | `remove` | empty |
| Other date/datetime fields (`DA`/`DT`) | `shiftDateByRandomNbOfDays` | empty |
| Time fields (`TM`) | `shiftTimeByRandom` | empty |
| Unix timestamp fields (`SL`/`FD`) | `shiftUnixTimeStampRandom` | empty |
| Age fields (`AS`) | `capAgeAt99IfOver90` | empty |
| Institution, organization, department, station, address, phone, email, or contact fields | `remove` | empty |
| Description, comments, protocol, reason, history, diagnosis, note, or other free-text fields (must be a `cleanTag`-eligible VR) | `cleanTag` | `deid` |
| Private tags or user-requested aggressive metadata cleanup | `delete` | empty |

If the tag's VR does not support the pattern-implied action (for example, a "description" field whose VR is not in `cleanTag`'s valid VR list), do not force that action. Fall back to a VR-agnostic action (`remove`, `delete`, or `replaceWithLiteral`) or ask the user.

For free-text fields, `cleanTag` and `remove` are the two realistic options, and they trade off cost against precision: `cleanTag` runs NER to find and handle PHI entities inside the text, which is comparatively expensive but preserves non-PHI content in the field. `remove` is much cheaper — no NER involved — but blunt: it empties the whole field regardless of what it actually contains. Default to `cleanTag`, but mention this tradeoff when presenting free-text rows, and switch to `remove` when the user wants a cheaper, coarser option or explicitly does not need NER.

When the user asks for pseudonymization or provides external replacement values, override the default mapping and use `replaceWithMapping,Nested` for mapped tags.

If a tag does not match the policy clearly, ask what should happen to that tag. Good question shape: `For (xxxx, yyyy) <Name> (VR <VR>), should I remove the value, delete the tag, hash it, replace it with a literal, clean it with NER, or map it from external values?`

### Boundaries and Verification

- Never invent a new `Action` name. Only use the actions listed in `Action → Valid VR Compatibility` above. If none of them fit a tag's purpose, do not create one — ask the user.
- Never invent an `Option` value beyond what is documented for that action (see `cleanTag Option Semantics`, `replaceWithRandomName Option Semantics`, and the `Option` column in the tables above). Never invent a default beyond `Default Mapping Policy`.
- If you are unsure which action applies to a specific tag, search the web for that tag's real meaning (DICOM standard definition, vendor documentation) before deciding. Do not guess from the tag name alone.
- Never assume a mapping is correct just because it looks reasonable. Always surface it to the user for confirmation before treating the strategy file as final — including rows you are confident about.

### Confidence Buckets

When generating a strategy file for more than a couple of tags, group the rows into three buckets and label them clearly for the user, in this order:

1. **Sure** — the tag/name/VR combination maps unambiguously to a documented default in the mapping policy.
2. **Web-based double check** — the mapping was not obvious from the tag/name/VR alone, so a web search was used to confirm what the tag represents before choosing an action; briefly note what was confirmed.
3. **Unknown** — no confident mapping was found even after a web search. Do not guess an action for these rows. List them separately and ask the user what to do with each one; do not include an unresolved row in the final CSV.

Present these three buckets as clearly labeled sections in your response (for example `# Sure`, `# Web-based double check`, `# Unknown — needs your input`), and always close by asking the user to review and confirm every bucket — including "Sure" — before the strategy file is treated as final.

A tag with no row in the strategy CSV at all is not de-identified. `DicomMetadataDeidentifier` only transforms tags it finds in the strategy file; every other tag passes through with its original value unchanged. This means every **Unknown** tag ships with its original, un-redacted value if the pipeline runs before it is resolved. Whenever you show an Unknown bucket, explicitly say this — do not let the user assume "unknown" means "safely skipped."

### Accepting User Input

If the user provides only `Tags`, `VR`, and `Name`, complete the strategy file on their behalf by mapping each row to `Action` and `Option`. Preserve the user's tag, VR, and name exactly. If the mapping is clear and the resulting `Action`/`VR` pair is valid, do not ask the user to choose Spark OCR action names. If any row is ambiguous, the VR is missing, or no VR-valid action clearly matches, ask a focused question before filling `Action` and `Option`; do not guess.

User tag input can arrive in any reasonable format: pasted text, messy notes, markdown tables, CSV/TSV, JSON, spreadsheets, file paths, DataFrame previews, notebook output, or bullet lists. Do not require exact column names or clean formatting. Accept variants such as `Tag`, `Tags`, `DICOM Tag`, `VR`, `Value Representation`, `Name`, `Keyword`, `Description`, or similar labels. First normalize the input into internal rows with `Tags`, `VR`, and `Name`; only the final generated strategy file must use the canonical `Tags,VR,Name,Status,Action,Option` CSV shape. If the input cannot be parsed confidently, ask the user for the missing or unclear fields instead of rejecting the format or guessing.

### Flow

1. Parse the user's raw input into internal rows with `Tags`, `VR`, and `Name`, regardless of the input format.
2. For each row, check whether it maps unambiguously to the `Default Mapping Policy`. If it does, place it in the **Sure** bucket.
3. If the mapping is not obvious from the tag/name/VR alone, search the web for the tag's real meaning before deciding. If that resolves it, place it in the **Web-based double check** bucket and note what was confirmed.
4. If still unresolved, place the row in the **Unknown** bucket. Do not guess an action for it.
5. For every Sure and Web-based-double-check row, validate its `VR` against the chosen action's valid-VR list. If it does not validate, pick a VR-agnostic fallback (`remove`, `delete`, `replaceWithLiteral`) or move the row to Unknown; never emit an invalid pairing.
6. Present all three buckets to the user and ask them to confirm every row — including Sure rows — before finalizing. Never assume confirmation.
7. Write only the confirmed rows to the canonical `Tags,VR,Name,Status,Action,Option` CSV using the creation pattern below. Never include Unknown rows in the CSV.

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

action_valid_vrs = {
    "replaceWithLiteral": None,
    "delete": None,
    "remove": None,
    "ensureTagExists": None,
    "replaceWithRandomName": {"PN", "LO"},
    "cleanTag": {"PN", "UI", "SH", "CS", "LO", "LT", "ST", "IS", "TM", "AE", "US", "AS", "DS", "OB", "DT", "OW", "UT"},
    "hashId": {"UI", "LO", "SH"},
    "patientHashId": {"LO"},
    "shiftDateByFixedNbOfDays": {"DA", "DT"},
    "shiftDateByRandomNbOfDays": {"DA", "DT"},
    "shiftTimeByRandom": {"TM"},
    "shiftUnixTimeStampRandom": {"SL", "FD"},
    "shiftAgeByRandom": {"AS"},
    "capAgeAt99IfOver90": {"AS"},
    "replaceWithMapping": None,
}


def is_valid_action_for_vr(action, vr):
    valid_vrs = action_valid_vrs.get(action)
    return valid_vrs is None or vr in valid_vrs


def infer_metadata_action(tag, vr, name, mode="phi_only"):
    name_lower = name.lower()

    clean_tag_terms = ("description", "comment", "protocol", "reason", "history", "diagnosis", "note")
    person_terms = ("physician", "doctor", "operator", "performer", "referring")
    identifier_terms = ("uid", "id", "identifier", "accession", "account", "admission", "issuer", "serial")
    organization_terms = ("institution", "organization", "department", "station", "address", "phone", "email", "contact")

    if mode == "pseudonym":
        candidate = ("replaceWithMapping", "Nested")
    elif "private" in name_lower or mode == "aggressive":
        candidate = ("delete", "")
    elif any(term in name_lower for term in clean_tag_terms):
        candidate = ("cleanTag", "deid")
    elif "birth" in name_lower and "date" in name_lower:
        candidate = ("remove", "")
    elif vr in ("DA", "DT") or "date" in name_lower:
        candidate = ("shiftDateByRandomNbOfDays", "")
    elif vr == "TM" or "time" in name_lower:
        candidate = ("shiftTimeByRandom", "")
    elif vr in ("SL", "FD") and "timestamp" in name_lower:
        candidate = ("shiftUnixTimeStampRandom", "")
    elif vr == "AS" or "age" in name_lower:
        candidate = ("capAgeAt99IfOver90", "")
    elif any(term in name_lower for term in person_terms):
        candidate = ("replaceWithRandomName", "")
    elif vr == "PN" or "patient name" in name_lower or "person name" in name_lower:
        candidate = ("replaceWithLiteral", "<REMOVED>")
    elif any(term in name_lower for term in identifier_terms):
        candidate = ("hashId", "")
    elif any(term in name_lower for term in organization_terms):
        candidate = ("remove", "")
    else:
        return None, None

    action, option = candidate
    if not is_valid_action_for_vr(action, vr):
        return None, None
    return action, option

csv_buffer = StringIO()
csv_buffer.write(dedent("""\
Tags,VR,Name,Status,Action,Option
"""))
csv_writer = csv.writer(csv_buffer)

for item in user_tags:
    action, option = infer_metadata_action(item["Tags"], item["VR"], item["Name"], mode="phi_only")
    if action is None:
        raise ValueError(f"No VR-valid action inferred for {item['Tags']} {item['Name']} (VR {item['VR']}). Ask the user before guessing.")
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
"(0011, 1010)",SL,Private Unix Timestamp,,shiftUnixTimeStampRandom,
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

external_mapping_df = spark.createDataFrame(data, dicomExternalSchema) \
    .withColumn("base_path", get_base_path(F.col("path"))) \
    .drop("path")
dicom_df = dicom_df.join(external_mapping_df, "base_path", "left")
```

Use this when the user wants to remove or delete whole DICOM groups. Group-level actions use the sentinel `VR` value `GROUP`, not a real DICOM VR, so the VR compatibility table above does not apply to them; only `remove` and `delete` are supported at the group level.

The group tag is a hex-digit *prefix* of the 4-digit DICOM group number, 1 to 3 digits long — `"(G,)"`, `"(GG,)"`, or `"(GGG,)"` — and it matches every group whose number starts with that prefix. Never use a full 4-digit prefix (`"(GGGG,)"`): that addresses exactly one group already reachable with a normal per-tag row, which defeats the purpose of a group-level wildcard. Prefer the shortest prefix that covers the intended range:

- `"(5,)"` (1 digit): every group `0x5000`-`0x5FFF`.
- `"(50,)"` (2 digits): Curve Data groups `0x5000`-`0x50FF`.
- `"(600,)"` (3 digits): Overlay Data groups `0x6000`-`0x600F`.

```python
from textwrap import dedent

csv_group_strategy_data = dedent("""\
Tags,VR,Name,Status,Action,Option
"(5,)",GROUP,None,,remove,
"(50,)",GROUP,None,,remove,
"(600,)",GROUP,None,,delete,
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
