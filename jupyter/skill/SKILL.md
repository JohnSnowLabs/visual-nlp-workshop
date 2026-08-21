---
name: visual-nlp
description: Build John Snow Labs Visual NLP workflows for DICOM, image, and PDF inputs, including OCR, PHI redaction, visible text redaction, DICOM metadata de-identification, encapsulated PDFs, model-backed baselines, slash task menus (/all_tasks, /all_actions, /dicom_tasks, /image_tasks, /pdf_tasks, /input_types), and Python-style Spark OCR / Healthcare NLP code examples.
---

# Visual NLP Skill

Use this skill for Visual NLP workflows across DICOM, image, and PDF inputs.

## First Response

After this skill loads, immediately show the Visual NLP action categories. Do not explain the skill, summarize capabilities, brainstorm options, ask clarifying questions, infer a route, or generate code before showing them. Fallback startup text is prohibited; the action categories view is the only allowed first response.

Always show the relevant list before selecting any Visual NLP route. Do not infer a default task from the user request, even when the user describes a concrete workflow. Stop after showing the list, then ask the user to choose a task by number or exact name, or to run one of the listed commands.

For ordinary Visual NLP prompts (no explicit command) or `/all_actions`, show only the action categories:

```text
Available Visual NLP actions:

- /all_tasks: list all Visual NLP tasks.
- /all_actions: list all Visual NLP actions.
- /dicom_tasks: list DICOM-only tasks.
- /image_tasks: list image-only tasks.
- /pdf_tasks: list PDF-only tasks.
- /input_types: list supported input types.
```

For `/all_tasks`, show only the full task list:

```text
I can help you build one of these Visual NLP workflows:

1. DICOM metadata inspection: extract DICOM metadata into a DataFrame; no changes made to the file.
2. DICOM metadata de-identification: de-identify structured metadata tags using a strategy file (hash, remove, replace, shift dates, etc.).
3. DICOM metadata strategy file creation: build a copy-ready strategy CSV mapping tags to actions, without running a full pipeline.
4. DICOM free-text metadata de-identification: de-identify PHI inside free-text metadata fields (e.g. study description) using cleanTag plus zero-shot NER models, allows stacking of NER models.
5. DICOM pixel PHI redaction with the common pixel builder: redact burned-in pixel PHI using a configurable OCR engine (VLM/V2/V3) and a Healthcare NLP clinical pipeline.
6. DICOM pixel PHI redaction with zero-shot NER: redact burned-in pixel PHI using VLM OCR plus zero-shot NER models, allows stacking of NER models.
7. DICOM remove-all visible text: blanket-redact all detected visible text on DICOM pixels; no OCR or NER, fastest/cheapest pixel option.
8. DICOM encapsulated PDF de-identification with the common pixel builder: redact PHI in a PDF stored inside DICOM, using the common pixel builder and a clinical pipeline.
9. DICOM encapsulated PDF de-identification with zero-shot NER: redact PHI in a PDF stored inside DICOM using VLM OCR plus zero-shot NER models, allows stacking of NER models.
10. DICOM pretrained baseline: fast, single-call Spark OCR pretrained pipeline for DICOM (minimal / pseudonym / full anonymization).
11. Image PHI redaction with the common pixel builder: redact PHI in standalone image files using a configurable OCR engine and a clinical pipeline.
12. Image PHI redaction with zero-shot NER: redact PHI in standalone image files using VLM OCR plus zero-shot NER models, allows stacking of NER models.
13. PDF PHI redaction with the common pixel builder: redact PHI in standalone PDF pages using a configurable OCR engine and a clinical pipeline.
14. PDF PHI redaction with zero-shot NER: redact PHI in standalone PDF pages using VLM OCR plus zero-shot NER models, allows stacking of NER models.
```

For `/dicom_tasks`, show:

```text
I can help you build one of these DICOM Visual NLP workflows:

1. DICOM metadata inspection: extract DICOM metadata into a DataFrame; no changes made to the file.
2. DICOM metadata de-identification: de-identify structured metadata tags using a strategy file (hash, remove, replace, shift dates, etc.).
3. DICOM metadata strategy file creation: build a copy-ready strategy CSV mapping tags to actions, without running a full pipeline.
4. DICOM free-text metadata de-identification: de-identify PHI inside free-text metadata fields (e.g. study description) using cleanTag plus zero-shot NER models, allows stacking of NER models.
5. DICOM pixel PHI redaction with the common pixel builder: redact burned-in pixel PHI using a configurable OCR engine (VLM/V2/V3) and a Healthcare NLP clinical pipeline.
6. DICOM pixel PHI redaction with zero-shot NER: redact burned-in pixel PHI using VLM OCR plus zero-shot NER models, allows stacking of NER models.
7. DICOM remove-all visible text: blanket-redact all detected visible text on DICOM pixels; no OCR or NER, fastest/cheapest pixel option.
8. DICOM encapsulated PDF de-identification with the common pixel builder: redact PHI in a PDF stored inside DICOM, using the common pixel builder and a clinical pipeline.
9. DICOM encapsulated PDF de-identification with zero-shot NER: redact PHI in a PDF stored inside DICOM using VLM OCR plus zero-shot NER models, allows stacking of NER models.
10. DICOM pretrained baseline: fast, single-call Spark OCR pretrained pipeline for DICOM (minimal / pseudonym / full anonymization).
```

For `/image_tasks`, show:

```text
I can help you build one of these image Visual NLP workflows:

1. Image PHI redaction with the common pixel builder: redact PHI in standalone image files using a configurable OCR engine and a clinical pipeline.
2. Image PHI redaction with zero-shot NER: redact PHI in standalone image files using VLM OCR plus zero-shot NER models, allows stacking of NER models.
```

For `/pdf_tasks`, show:

```text
I can help you build one of these PDF Visual NLP workflows:

1. PDF PHI redaction with the common pixel builder: redact PHI in standalone PDF pages using a configurable OCR engine and a clinical pipeline.
2. PDF PHI redaction with zero-shot NER: redact PHI in standalone PDF pages using VLM OCR plus zero-shot NER models, allows stacking of NER models.
```

For `/input_types`, show:

```text
Supported Visual NLP input types:

1. DICOM.
2. Image.
3. PDF.
```

For ordinary Visual NLP prompts with no explicit command, or for `/all_actions`, show only the action categories. For `/all_tasks`, show only the full task list. For `/dicom_tasks`, `/image_tasks`, and `/pdf_tasks`, show only the matching focused task list. For `/input_types`, show only supported input types. Never combine the action categories with a task or input-type list in the same response. Generate code only after the user chooses a task by number or exact name.

## Read Order

1. Read `references/routing.yaml`.
2. Show the action categories or the relevant task list first unless the immediately preceding user message already selected a task by number or exact name.
3. Select one route by the user's chosen task, or more than one route when the request clearly spans multiple non-conflicting routes for the same input type (see `Combining Routes`).
4. Read only the files listed in the selected route's `read` list.
5. If the selected route has `template_sections`, use those sections of the `template_file` in order. If it has `template_section`, use only that section.
6. Read `references/models.yaml` when the selected route uses pretrained/model-backed stages. If the user has not specified a config for a model group, or asks which one to pick, show that group's `selection_guide` before defaulting.
7. Read `references/checkpoints.yaml` and validate the selected route's checkpoints.
8. When a route reads `common/utilities.md`, include only the `common_display`, `common_save`, or metadata utility section that matches the selected input type, task, and output column. Include `intermediate_display_utility` only when the user explicitly asks to inspect intermediate OCR text, regions, coordinates, or page images.
9. When a route reads `common/template-zero-shot-builder.md`, include `common_zero_shot_config` and `common_zero_shot_ner_builder` before the input-specific workflow template; use `common_zero_shot_two_model_config` instead of `common_zero_shot_config` when the user asks for two stacked zero-shot NER models. Include `common_zero_shot_position_finder` from `common/visual_stage.md`.
10. When a Pixel Builder route reads `common/template-pixel-builder-common.md`, include `common_pixel_builder_helpers` before the selected DICOM or image/PDF implementation template.
11. For Pixel Builder and zero-shot NER routes that offer an OCR engine choice, output only the OCR helper and branch needed by the selected engine. If the config uses VLM, include only the VLM OCR helper and make the pipeline call that helper directly. Do not output the non-VLM helper or an unused non-VLM branch. If the config uses V1, V2, or V3, include only the non-VLM helper and branch.
12. When a route reads `dicom/metadata-creation.md`, create every strategy CSV, group strategy CSV, or `cleanTag` CSV with its `metadata_creation_pattern` code shape and statement order.

Do not browse folders manually. Let `references/routing.yaml` choose the common, DICOM, image, or PDF files to read.

## Routing Policy

- Do not select a route from the prompt until the user chooses a task from the shown list.
- Map the chosen task number(s) or exact name(s) to one or more routes in `references/routing.yaml`.
- Use DICOM metadata and strategy files only for DICOM routes.
- Use common OCR, NER, visual stage, imports, model, and redaction references for every input type that needs them.

## Combining Routes

Most requests map to exactly one route. Do not force an either/or choice between two routes when the user's request genuinely needs both — combine them into one answer instead:

- The user may select more than one task at once (e.g. by number, "and", or a request that names two workflows), or a single generated pipeline may need a second route's stage tacked on (e.g. pixel PHI redaction plus metadata de-identification for private/overlay tags).
- Only combine routes that share the same `input_type` and do not conflict. Two routes conflict when they'd both claim the same terminal stage or output column with incompatible behavior — for example, `pixel_remove_all_text` (blanket redaction) and `pixel_phi_zero_shot`/`pixel_phi_builder` (PHI-only redaction) both produce `dicom_pixel_cleaned` via `DicomDrawRegions` but redact different scopes; these cannot be combined; ask the user to pick one.
- Non-conflicting combinations produce different output columns and can be chained in one pipeline — most commonly a pixel-redaction route (`pixel_phi_builder`, `pixel_phi_zero_shot`) followed by `metadata_deid`, matching the existing precedent in `dicom.pixel_phi_builder`'s `config["apply_metadata_deid"]` option: pixel redaction runs first, producing `dicom_pixel_cleaned`; `DicomMetadataDeidentifier` then consumes that column and produces the final `dicom_metadata_cleaned`.
- When combining, read every file listed for every selected route, apply each route's own template/section rules, and state briefly which routes are being combined before generating code. Keep the result as one contiguous `Pipeline`/`PipelineModel` block per `common/formatting.md`, not separate code blocks per route.
- If it is unclear whether the user wants both routes or is choosing between them, ask — do not guess.

## Default Policy

- After the user selects a task, use GPU and pretrained/model-backed OCR or NER stages unless the user asks for CPU-only, cheaper OCR, custom stages, or no pretrained models.
- When a route offers an `ocr_engine` choice, pick it from `models.yaml` `ocr_engine.selection_guide.default_by_hardware` based on GPU vs CPU: on GPU, default to VLM (offer V2 only if the user explicitly asks for a different GPU-capable engine); on CPU, default to V1 for image, PDF, and DICOM encapsulated PDF routes, or V3 for normal DICOM pixel routes (`pixel_phi_builder`, `pixel_phi_zero_shot`, `pixel_remove_all_text`) since V1 is never valid there — offer V2/V3 (or V2) as alternatives only if the user explicitly asks for a different CPU engine. When a `detector_engine` sub-choice applies, default to `V1` (`ImageTextDetector`); offer `V2` only if asked.
- After the user selects a redaction task, use PHI-only redaction unless the selected route is DICOM remove-all visible text.
- Use the common pixel builder for selected PHI redaction builder workflows across DICOM, image, and PDF input types.
- Keep input-specific byte/file contracts separate: DICOM routes produce DICOM bytes, image routes produce image outputs, and PDF routes produce PDF outputs.
- Keep DICOM, image, and PDF adapter code separate inside the common pixel builder route.
- Do not mix stage-level `PretrainedZeroShotNER` into common pixel builder routes unless the user explicitly asks for zero-shot or configurable entity coverage.

## Output Style

- After a task is selected, follow `common/formatting.md` `Formatting Rules` and `Final Code Formatting Gate` for every generated workflow answer.
- Before showing any `Pipeline(...)` or `PipelineModel(...)` code block, give a numbered list describing what each stage in that pipeline does, in stage order.
- Default output is copy-ready notebook blocks in chat. Do not create or mention a file artifact unless the user explicitly asks for one.
