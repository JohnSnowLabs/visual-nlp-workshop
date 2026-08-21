# Strategy Files

Use strategy files with `DicomMetadataDeidentifier`.

## Default Response Behavior

When the user asks about `DicomMetadataDeidentifier`, `DicomMetadataDeIdentifier`, metadata de-identification, or strategy files, show the full action catalog before suggesting a starter CSV. Do not show only the actions used in the example.

Use this order:
1. Show the metadata action catalog.
2. Mention the group-level action catalog separately.
3. If the user provided `Tags`, `VR`, and `Name`, map `Action` and `Option` for them using only the documented actions and options, sorting rows into the Sure / Web-based double check / Unknown buckets (see `Confidence Buckets`) instead of guessing.
4. Then give the starter strategy file that matches the request, and ask the user to confirm every bucket, including Sure, before treating it as final.

## Shape

```csv
Tags,VR,Name,Status,Action,Option
"(0010, 0010)",PN,Patient Name,,replaceWithLiteral,<REMOVED>
"(0010, 0020)",LO,Patient ID,,hashId,
"(0008, 1030)",LO,Study Description,,cleanTag,deid
```

When the user provides only `Tags`, `VR`, and `Name`, preserve those values exactly and infer `Action` and `Option` from the tag name, VR, and requested mode. User-provided tags can arrive as pasted text, messy notes, markdown tables, CSV/TSV, JSON, spreadsheets, file paths, DataFrame previews, notebook output, or bullet lists. Do not apply hard rules to the input format or require exact column names; normalize flexible inputs into internal `Tags`, `VR`, and `Name` rows first. Use the `Default Mapping Policy` below, validated against `Action → Valid VR Compatibility`. If any row is ambiguous, unparseable, missing required fields, or the model is unsure, ask a focused question before filling `Action` and `Option`; do not guess.

## Boundaries and Verification

- Never invent a new `Action` name. Only use the actions listed in `Metadata Action Catalog` / `Action → Valid VR Compatibility`. If none of them fit a tag's purpose, do not create one — ask the user.
- Never invent an `Option` value beyond what is documented for that action (see the `cleanTag` and `replaceWithRandomName` sections below and the `Option` column in the catalog). Never invent a default beyond `Default Mapping Policy`.
- If you are unsure which action applies to a specific tag, search the web for that tag's real meaning (DICOM standard definition, vendor documentation) before deciding. Do not guess from the tag name alone.
- Never assume a mapping is correct just because it looks reasonable. Always surface it to the user for confirmation before treating the strategy file as final — including rows you are confident about.

## Confidence Buckets

When generating a strategy file for more than a couple of tags, group the rows into three buckets and label them clearly for the user, in this order:

1. **Sure** — the tag/name/VR combination maps unambiguously to a documented default in the mapping policy.
2. **Web-based double check** — the mapping was not obvious from the tag/name/VR alone, so a web search was used to confirm what the tag represents before choosing an action; briefly note what was confirmed.
3. **Unknown** — no confident mapping was found even after a web search. Do not guess an action for these rows. List them separately and ask the user what to do with each one; do not include an unresolved row in the final CSV.

Present these three buckets as clearly labeled sections in your response (for example `# Sure`, `# Web-based double check`, `# Unknown — needs your input`), and always close by asking the user to review and confirm every bucket — including "Sure" — before the strategy file is treated as final.

A tag with no row in the strategy CSV at all is not de-identified. `DicomMetadataDeidentifier` only transforms tags it finds in the strategy file; every other tag passes through with its original value unchanged. This means every **Unknown** tag ships with its original, un-redacted value if the pipeline runs before it is resolved. Whenever you show an Unknown bucket, explicitly say this — do not let the user assume "unknown" means "safely skipped."

## Default CSV Creation Pattern

For `strategy_file_path`, `group_strategy_file_path`, `cleanTag`, and `replaceWithMapping`, always create the CSV with `textwrap.dedent`, save it to disk, and pass the path to the DICOM stage. Create normal strategy files, group strategy files, and `replaceWithMapping` external mapping DataFrames before defining any pipeline or pipeline builder call that consumes them. This keeps generated examples easy to inspect, edit, and reuse. Use the canonical `metadata_creation_pattern` code shape from `metadata-creation.md` to create and save the CSV.

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
| `ensureTagExists` | Adds the tag empty if it is missing; otherwise leaves it as-is. | empty |
| `hashId` | Deterministically hashes an identifier; coherent by default, so the same original value always hashes to the same output. | empty |
| `patientHashId` | Creates a patient-specific identifier hash; coherent by default, so the same original value always hashes to the same output. | empty |
| `replaceWithLiteral` | Replaces the tag value with the literal in `Option`. | replacement text |
| `replaceWithMapping` | Replaces the value using external mapping data. | `Nested` when nested mapping is needed |
| `replaceWithRandomName` | Replaces the value with a generated random person name. | empty, or `coherent` to make the same original name consistently map to the same random name |
| `shiftDateByFixedNbOfDays` | Shifts dates by a fixed number of days. If `Option` is missing or not a valid integer, silently falls back to `shiftDateByRandomNbOfDays` behavior instead. | signed integer (e.g. `5` or `-5`); positive shifts forward, negative shifts backward |
| `shiftDateByRandomNbOfDays` | Shifts dates by a random number of days. | empty |
| `shiftTimeByRandom` | Randomly shifts time values. | empty |
| `shiftUnixTimeStampRandom` | Randomly shifts Unix timestamp values. | empty |
| `shiftAgeByRandom` | Randomly shifts age values. | empty |
| `capAgeAt99IfOver90` | Caps ages above 90 at 99. | empty |

Do not mention or include omitted legacy actions in user-facing responses or examples.

## Action → Valid VR Compatibility

This is a hard constraint, taken from the `DicomMetadataDeidentifier` action dispatch table. An `Action` only works on the VRs listed for it; assigning an action to a tag whose VR is not listed fails at runtime. Never emit a strategy row whose `Action` is not valid for that row's `VR`. If a tag's VR is unknown, do not guess an action that depends on VR — ask, or use a VR-agnostic action.

| Action | Valid VRs |
|---|---|
| `replaceWithLiteral` | Any VR |
| `delete` | Any VR |
| `remove` | Any VR |
| `ensureTagExists` | Any VR |
| `replaceWithMapping` | Any VR (external mapping value replaces the tag directly; not VR-dispatched) |
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

Group-level rows use the sentinel `VR` value `GROUP`, not a real DICOM VR, so this table does not apply to them; only `remove` and `delete` are supported at the group level.

## Default Mapping Policy

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

## Group-Level Strategy Actions

Group-level actions are not normal per-tag actions. Use a separate group strategy file when all tags in a DICOM group should be removed or deleted, including nested tags.

Supported group-level actions:
- `remove`: remove values for all tags in the group.
- `delete`: delete all tags in the group.

CSV shape:

```csv
Tags,VR,Name,Status,Action,Option
"(5,)",GROUP,None,,remove,
"(50,)",GROUP,None,,remove,
"(600,)",GROUP,None,,delete,
```

The group tag is a hex-digit *prefix* of the 4-digit DICOM group number, 1 to 3 digits long — `"(G,)"`, `"(GG,)"`, or `"(GGG,)"` — and it matches every group whose number starts with that prefix. Never use a full 4-digit prefix (`"(GGGG,)"`): that addresses exactly one group already reachable with a normal per-tag row, which defeats the purpose of a group-level wildcard. Prefer the shortest prefix that covers the intended range:

- `"(5,)"` (1 digit): every group `0x5000`-`0x5FFF`.
- `"(50,)"` (2 digits): Curve Data groups `0x5000`-`0x50FF`.
- `"(600,)"` (3 digits): Overlay Data groups `0x6000`-`0x600F`.

Use `remove` to remove group values while keeping tags, and use `delete` to delete all tags in the group.

Pass a group strategy file path:

```python
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

The metadata strategy CSV that uses `replaceWithMapping` must still be written to disk first with the default CSV creation pattern. Create the external mapping DataFrame before defining any pipeline that consumes `external_mapping`.

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

external_mapping_df = spark.createDataFrame(data, dicomExternalSchema) \
    .withColumn("base_path", get_base_path(F.col("path"))) \
    .drop("path")
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

`cleanTag` requires:
- `DicomToMetadata.setExtractTagForNer(True)`
- `tag_text`
- `tag_mapping`
- Healthcare NLP output `deid_documents`
- `DicomMetadataDeidentifier.setTagCleanedCol("deid_documents")`

## replaceWithRandomName

- empty: each occurrence gets an independently generated random name, even for the same original value.
- `coherent`: the same original name always maps to the same random replacement name, so repeated occurrences of one person stay consistent.

Default to empty unless the user asks for consistent/coherent name replacement across rows or documents.
