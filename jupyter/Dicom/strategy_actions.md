# Usage Example

[Metadata De-identification Notebook](https://github.com/JohnSnowLabs/visual-nlp-workshop/blob/master/jupyter/Dicom/SparkOcrMetadataDeIdentification.ipynb)

---

# Per-Tag Actions

| Action | VR(s) | Description |
|---|---|---|
| **`replaceWithRandomName`** | `PN`, `LO` | Replaces the value with a random **"First Last"** name. If `userSeed` is set and `Option` is `coherent` (default), the same original value always maps to the same random name. Setting `Option` to `random` disables this determinism. |
| **`replaceWithDummyGraphicalAnnotation`** | `SQ` | Replaces a Graphic Annotation Sequence with a fixed dummy annotation consisting of a circle graphic and an `"ANONYMOUS"` text item. Not present in any current CSV. |
| **`replacewithDummyVerifyingObserverIdentificationSequence`** | `SQ` | Replaces a Verifying Observer Identification Sequence with a dummy observer (`"ANONYMOUS"` name/organization and a dummy code sequence). Not present in any current CSV. |
| **`replaceWithDummyPersonIdentificationCode`** | `SQ` | Replaces a Person Identification Code Sequence with a single dummy `"Anonymized"` code item. Not present in any current CSV. |
| **`replaceWithLiteral`** | `ALL` | Substitutes a fixed literal from the `Option` column. For numeric VRs, the literal must parse correctly and fit the VR's value range; otherwise, it falls back to blank. For binary VRs (`OB`, `OW`, `OF`, `OD`, `OL`, `OV`) and `SQ`, the literal is ignored and the VR-specific default/empty value is used instead. |
| **`cleanTag`** | `PN`, `UI`, `SH`, `CS`, `LO`, `LT`, `ST`, `IS`, `TM`, `AE`, `US`, `AS`, `DS`, `OB`, `DT`, `OW`, `UT` | Entry point for the NER/regex PHI pipeline. Marks the tag so its value is later replaced with the corresponding NER-cleaned text produced upstream via `DicomToMetadata` and a document-classification/NER stage. The sub-option `remove` / `deid` / `mask` controls how `<PLACEHOLDER>` spans in the cleaned text are rendered: stripped, left as-is, or masked with `*`. |
| **`delete`** | `ALL` (including `SQ`) | Deletes the tag entirely — both the key and its value are removed from the dataset. |
| **`remove`** | `ALL` | Clears the value while keeping the tag present. Behavior is VR-specific: string VRs → `""`; `DA`/`DT`/`TM` → `""`; numeric VRs (`US`/`SS`/`UL`/`SL`/`UV`/`SV`/`FL`/`FD`/`AT`) → `0`/`0.0`; binary VRs (`OB`/`OD`/`OF`/`OW`/`UN`/`OL`/`OV`) → a single-space byte; `SQ` → `[]`. |
| **`ensureTagExists`** | `ALL` | If the tag exists with a value, this is a no-op. If it exists but is empty, it is filled with a VR-specific default. If the tag is missing entirely, it is created with that default. |
| **`patientHashId`** | `LO` | SHA-1 hashes the original Patient ID and maps the resulting hash digits back into the same length as the original, avoiding a leading zero. Produces a consistent anonymized ID of identical length. |
| **`hashId`** | `UI`, `LO`, `SH` | Deterministically hashes the value using UUID5 with a fixed namespace/salt, producing a valid DICOM UID of the form `2.25.<int>` (≤64 characters and with a non-zero leading digit). |
| **`shiftUnixTimeStampRandom`** | `SL`, `FD` | Shifts a Unix timestamp by a random number of days into the past (1–60 days). The `Option` value is not used. |
| **`shiftDateByRandomNbOfDays`** | `DA`, `DT` | Infers the date format from the value's length/pattern and shifts it by a random offset. `Option` specifies the maximum range in days. Year/year-month precision values use a larger 365–730 day range. Falls back to a fully random date if the value cannot be parsed. |
| **`shiftDateByFixedNbOfDays`** | `DA`, `DT` | Same format inference as `shiftDateByRandomNbOfDays`, but shifts the date by the fixed number of days specified in `Option`. If `Option` is not a valid integer, it falls back to a random offset. If the value cannot be parsed, it generates a random date. This produces deterministic shifts when given a valid `Option`. |
| **`shiftTimeByRandom`** | `TM` | Replaces the value with a random `HHMMSS` time. Fractional seconds are added if `Option` is `fractional` or if the original value contained a decimal point. The result is truncated or padded to match the original length. |
| **`shiftAgeByRandom`** | `AS` | Replaces the value with a random age string in DICOM `AS` format, e.g. `045Y` (digits followed by a `D`/`W`/`M`/`Y` unit). |
| **`capAgeAt99IfOver90`** | `AS` | If the value is valid DICOM `AS` format and represents an age in years greater than 90, caps it at `099Y`. Otherwise, the value is left unchanged. If the value is not valid `AS` format, falls back to `shiftAgeByRandom`. |
| **`replaceWithMapping`** | `ALL` | Replaces the value using an externally supplied per-tag mapping (`externalMapping` column/parameter), with type conversion according to `Option.value_type` (`int`/`float`/`str`). The nested `Option` allows the mapping to be applied inside sequence items as well; otherwise, only top-level occurrences are modified. |

# Minimal Strategy File

```text
Tags,VR,Name,Status,Action,Option
"(0010, 0010)",PN,Patient Name,,replaceWithLiteral,<REMOVED>
"(0010, 0020)",LO,Patient ID,,patientHashId,
"(0010, 0030)",DA,Patient Birth Date,,remove,
"(0010, 0040)",CS,Patient Sex,,remove,
"(0010, 1010)",AS,Patient Age,,capAgeAt99IfOver90,
"(0008, 0020)",DA,Study Date,,shiftDateByFixedNbOfDays,3
"(0008, 0030)",TM,Study Time,,shiftTimeByRandom,
"(0008, 0050)",SH,Accession Number,,hashId,
"(0008, 0080)",LO,Institution Name,,delete,
"(0008, 0090)",PN,Referring Physician Name,,replaceWithRandomName,
"(0008, 1030)",LO,Study Description,,cleanTag,deid
"(0018, 1030)",LO,Protocol Name,,cleanTag,deid
"(0020, 000D)",UI,Study Instance UID,,hashId,
"(0020, 000E)",UI,Series Instance UID,,hashId,
"(0008, 0018)",UI,SOP Instance UID,,hashId,
```

---

# Group-Level Actions

These actions are defined separately in `groupStrategyFileContent` or `.setGroupStrategyFile(group_strategy_file_path)`.

For group-level actions:

- **VR column:** literal `GROUP`
- **Tag column:** a group prefix, e.g. `(60,)`

| Action | Description |
|---|---|
| **`delete`** | Deletes every tag whose group number matches the specified group prefix. |
| **`remove`** | Clears the value (sets it to `""`) of every tag whose group number matches the specified group prefix. |

# Minimal Group Strategy File

```text
Tags,VR,Name,Status,Action,Option
"(60,)",GROUP,None,,delete,
"(501,)",GROUP,None,,delete,
"(20,)",GROUP,None,,remove,
```
