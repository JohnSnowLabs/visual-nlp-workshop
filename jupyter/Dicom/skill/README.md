# Visual NLP DICOM Skill

This skill helps agents build John Snow Labs Visual NLP DICOM workflows with minimal friction. It focuses on choosing the right de-identification path, using grounded Spark OCR / Healthcare NLP stage contracts, and producing copy-ready code for DICOM metadata, pixel, and encapsulated PDF workflows.

## What It Helps Build

- Metadata inspection: view DICOM tags without changing files.
- Metadata de-identification: clean structured tags with strategy CSV files.
- Metadata strategy files: accept user-provided tags in loose formats and map `Action` / `Option`.
- Free-text metadata de-identification: clean text-like metadata fields with `cleanTag` and NER.
- Pixel PHI redaction with the DICOM pipeline builder.
- Pixel PHI redaction with stackable zero-shot NER models.
- Blanket visible-text removal with `ImageTextDetector` or `ImageTextDetectorV2`.
- Encapsulated PDF de-identification inside DICOM.
- Spark OCR pretrained DICOM baselines.

## Folder Layout

```text
dicom_skill/
├── SKILL.md
└── references/
    ├── routing.yaml
    ├── stages.md
    ├── templates.md
    ├── template-metadata.md
    ├── template-strategy-file.md
    ├── template-zero-shot.md
    ├── template-pixel-builder.md
    ├── template-pixel-blanket.md
    ├── template-encapsulated-pdf.md
    ├── template-pretrained.md
    ├── strategy-files.md
    ├── models.yaml
    ├── zero-shot-models.md
    ├── checkpoints.yaml
    └── faq.md
```

## How The Skill Works

`SKILL.md` is the entry point. It defines the first response, read order, default policy, formatting rules, imports, and pipeline architecture rules.

`routing.yaml` chooses the pipeline route. Each route points to a `template_file`, required models, strategies, and checkpoints.

`stages.md` defines stage contracts: columns, inputs, outputs, keep-input rules, VLM settings, utility functions, and stage-specific caveats.

Route-specific templates contain copy-ready code. Agents should copy from the selected template instead of assembling pipeline code from memory.

`checkpoints.yaml` is the final QA checklist before an answer is returned.

## Strategy File Flow

Use `template-strategy-file.md` when users ask to create, design, or review metadata strategy files.

Users can provide DICOM tag details in loose formats, including pasted text, tables, CSV/TSV, JSON, spreadsheets, DataFrame previews, notebook output, or files. The agent should normalize input into internal `Tags`, `VR`, and `Name` rows, then map `Action` and `Option`.

If a mapping is ambiguous, the agent must ask a focused question instead of guessing.

Final strategy files must use:

```csv
Tags,VR,Name,Status,Action,Option
```

## Important Defaults

- Default to pretrained/GPU workflows unless the user asks otherwise.
- Default visible text policy is PHI-only.
- Use the DICOM pipeline builder first for pixel PHI or pixel plus metadata workflows.
- Always read DICOM files as a Spark DataFrame before pipeline code.
- Use `PipelineModel` for outer DICOM/OCR/PDF workflows.
- Wrap NER subpipelines in functions that fit on an empty DataFrame and return a fitted `PipelineModel`.
- Never drop `path`.
- For final DICOM outputs, validate with `display_dicom(...)` and include `save_dicom_to_disk(...)`.

## Formatting Rules

Generated code should prioritize readability:

- Keep `.pretrained(...)` calls on one line.
- Keep `.setInputCols(...)` on one line.
- Use backslash chaining for Spark OCR and Healthcare NLP stages.
- Keep short setters, scalar params, enum params, helper calls, and short lists on one line.
- Name the main config dictionary `config`.

## Model And Pipeline References

Use `models.yaml` for model catalogs:

- Spark OCR pretrained DICOM pipelines.
- Healthcare NLP clinical pretrained pipelines used by the DICOM pipeline builder.
- Zero-shot NER models and entity coverage.
- OCR/VLM model options.

Use `zero-shot-models.md` for instructions on stacking zero-shot models and wiring them into pixel, PDF, or metadata cleanTag workflows.

## Source Policy

Do not guess stage behavior, model behavior, parameters, defaults, columns, or pipeline architecture.

Use local skill references and local workshop examples first. If external John Snow Labs documentation or model cards are used, tell the user that the information came from an external source and may need verification against their installed version.

## Using With Codex, Claude, And Gemini

Zip this folder and upload that ZIP directly to Claude, Codex, or Gemini.

The ZIP must include:

- `SKILL.md`
- the full `references/` folder

After uploading the ZIP, use this starter prompt:

```text
I uploaded a ZIP containing a DICOM skill. Use the ZIP as the source of truth.

First read SKILL.md. Then follow the read order in SKILL.md.

Select the workflow from references/routing.yaml. Read references/stages.md, the selected template_file, and any required model, strategy, zero-shot, FAQ, or checkpoint reference files.

Generate code only from the skill files. Do not invent stage inputs, outputs, columns, model names, or pipeline architecture. If anything is unclear, ask before guessing.
```

To see the workflow menu again after the ZIP is loaded, send:

```text
/dicom_tasks
```

## Maintenance Notes

- Add new workflows to `routing.yaml`, then create or update the corresponding `template-*.md`.
- Add new models to `models.yaml`; keep usage instructions in the relevant template or `zero-shot-models.md`.
- Add new strategy behavior to both `strategy-files.md` and `template-strategy-file.md`.
- Add or update checkpoints whenever a route introduces a new required behavior.
- Keep `SKILL.md` concise; put detailed examples in `references/`.
