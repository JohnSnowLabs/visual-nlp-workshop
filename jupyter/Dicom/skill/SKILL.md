---
name: visual-nlp-dicom
description: Build John Snow Labs Visual NLP DICOM de-identification, metadata inspection, pixel redaction, encapsulated PDF, and pretrained baseline workflows with minimal friction.
---

# Visual NLP DICOM Skill

Use this skill for DICOM metadata inspection, metadata de-identification, burned-in pixel PHI redaction, encapsulated PDF de-identification, blanket visible-text redaction, and Spark OCR pretrained DICOM baselines.

## First Response

Show the numbered options list only once: use it for the first DICOM-skill response in a conversation, regardless of how specific the user's prompt is. Do not repeat it on follow-up turns after the user is already working inside the DICOM workflow.

If the user sends `/dicom_tasks`, show the numbered options list again and stop there unless the same message also asks for a concrete workflow or code.

To reduce first-response lag, show this options list before reading `references/routing.yaml` or any route-specific reference files. After the list is visible, continue with routing and answering only if the user's message asked for a concrete workflow, code, or explanation.

Use this as the default first response shape:

```text
I can help you build one of these DICOM workflows:

1. Metadata inspection: view DICOM tags without changing the file.
2. Metadata de-identification: clean structured tags with a strategy file.
3. Create a metadata strategy file: choose actions such as remove, delete, hashId, cleanTag, replaceWithLiteral, replaceWithMapping, and group delete.
4. Free-text metadata de-identification: remove PHI from text-like metadata fields.
   Examples:
   - Series Description: Brain MRI for John Smith
   - Study Comments: call Dr. Adams at 555-1234
5. Pixel PHI redaction with the pipeline builder: build an end-to-end example using strong pretrained de-identification pipelines.
6. Pixel PHI redaction with zero-shot NER: stack multiple configurable NER models for experimentation.
7. Remove all visible text: blanket-redact detected text without OCR/NER.
8. Encapsulated PDF de-identification: redact PHI inside PDFs stored in DICOM.
9. Pretrained baseline: run a minimal ready Spark OCR DICOM de-identification pipeline.
```

If the user already gave a clear request on the first DICOM-skill response, show the options list once, then continue selecting the pipeline and generating the answer or code. On later follow-ups, skip the list and continue directly with the requested change or answer. If the follow-up is `/dicom_tasks`, show only the options list.

## Read Order

1. Read `references/routing.yaml`.
2. Select one `pipeline_id` from `pipelines`.
3. Ask a question only when the requested surface or output cannot be safely inferred and no safe default exists.
4. Read `references/stages.md` for the selected pipeline's stage contracts.
5. Read the selected pipeline's `template_file` from `references/routing.yaml`.
6. Read `references/models.yaml` only when the template uses pretrained/model-backed stages.
7. Read `references/strategy-files.md` only when metadata strategy actions are needed.
8. Read `references/checkpoints.yaml` and validate the selected pipeline's checkpoints.
9. Read `references/faq.md` when the user asks what operations are available, which workflow to choose, or common DICOM de-identification questions.
10. Read `references/zero-shot-models.md` when the user asks for zero-shot NER, stacked NER models, or configurable PHI detection for pixel or metadata de-identification. Use `references/models.yaml` for the zero-shot model catalog, links, and entity lists.
11. If the user is confused about, asks about, compares, or requests parameters for any NER-related stage, read `references/stages.md` `NER External References` and use the linked JSL/Spark NLP resource before responding.

Do not assemble code from memory. Use `routing.yaml` to choose the pipeline, `stages.md` to avoid bad columns, and the selected `template_file` to emit code.

## Source Transparency

- Guessing is wrong. Do not guess stage behavior, parameters, defaults, model usage, columns, or pipeline architecture.
- Always check local JSL workshop resources, skill references, or official JSL/Spark NLP docs before answering any uncertain DICOM, Spark OCR, Healthcare NLP, or NER question.
- For NER-related stage questions, always use the external link from `references/stages.md` `NER External References` instead of guessing.
- If using notebooks or files from the local workspace, say the information came from local workshop/notebook resources.
- If unsure about a DICOM, Visual NLP, Spark OCR, or Healthcare NLP detail, search the local COM Visual Repair Workshop repository first using local files/notebooks before guessing or using external sources.
- If the local checkout is missing context, use the COM Visual Repair Workshop DICOM repo link before broader external search: https://github.com/JohnSnowLabs/visual-nlp-workshop/tree/master/jupyter/Dicom
- Treat the local workshop repository as the first place to look for grounded examples, stage columns, parameter usage, utility functions, and pipeline architecture.
- If using information from an external source, such as a John Snow Labs model card, documentation page, blog post, release note, or web search result, explicitly say it came from an external source.
- When using external information, include the source link when possible.
- Add a short caveat that external information may change or may not be 100% correct, so users should verify against their installed library/model version for production use.
- Do not present externally sourced model descriptions, entity lists, defaults, or usage snippets as if they were inferred only from the skill files.

## Default Policy

- Default to GPU and pretrained/model-backed DICOM pipelines unless the user asks for CPU-only, cheaper OCR, custom stages, or no pretrained models. Do not ask a hardware question by default.
- Always include code that reads DICOM files into a Spark DataFrame before pipeline code: `dicom_df = spark.read.format("binaryFile").load(dicom_path)`.
- First suggest the configurable DICOM pipeline builder for pixel PHI or pixel plus metadata workflows.
- When using or recommending the DICOM pipeline builder, always output the complete builder code from `template-pixel-builder.md`, including `ocr_vlm_pipeline_builder`, `ocr_non_vlm_pipeline_builder`, `nlp_builder`, `dicom_pipeline_builder`, config, model loading, transform call, and optional inspection pass when relevant. Do not output only the `dicom_pipeline_builder(...)` call-site.
- Use direct custom stages when the user asks for a specific architecture, cheaper OCR path, blanket redaction, metadata-only de-identification, or encapsulated PDF processing.
- Metadata inspection and deterministic metadata-only de-identification do not need GPU or pretrained models.
- Default visible text policy is PHI-only. Use remove-all text only when requested.
- Keep intermediate DataFrames when the user wants inspection. If `DicomDrawRegions` is included, use a separate inspection pipeline that stops before `DicomDrawRegions`; do not claim pre-aggregation columns can be selected from the post-`DicomDrawRegions` result.
- For metadata inspection with a single metadata column, use `build_metadata_df(result, metadata_col="metadata_original")`; do not use broad `result.select(...)` examples for user-facing metadata inspection.
- For metadata comparison, use `build_metadata_comparison_df(result)` when both `metadata_original` and `metadata_cleaned` exist.
- For final DICOM-producing workflows, use `display_dicom(...)` for validation and include `save_dicom_to_disk(...)` with the final DICOM bytes column. Do this for every workflow except metadata inspection. When showing `save_dicom_to_disk(...)`, include the simple utility function from `references/stages.md` unless it was already defined earlier in the answer. The utility should only create the save directory if needed, extract DICOM bytes, keep the same base filename from `path`, save to disk, and return saved paths.
- When VLM is used for OCR, the image extraction/handling stage must set compressed image schema and dimensions: `BinaryToImage().setCompressImage(True).setImageDimsCol("frame_dims")`, `PdfToImage().setCompressImage(True).setImageDimsCol("frame_dims")`, or `DicomToImageV3().setCompressImage(True).setFrameDimsCol("frame_dims")`. If the OCR path is not VLM, set `setCompressImage(False)` on the image extraction stage.
- Always use the full canonical `MedicalVisionLLM` parameter block from `references/stages.md`; do not shorten it to only input/output columns or only `setNGpuLayers(99)`.
- When the user asks about `DicomMetadataDeidentifier`, `DicomMetadataDeIdentifier`, metadata de-identification, or strategy files, always show the full supported metadata action catalog from `references/strategy-files.md` before giving a starter strategy file.
- For external mapping / `replaceWithMapping`, always use `dicomExternalSchema` from `sparkocr.schemas` and the external mapping pattern in `references/strategy-files.md`.
- When generated code uses `strategy_file_path`, `group_strategy_file_path`, `cleanTag`, or `replaceWithMapping`, create the strategy CSV content with `textwrap.dedent`, write it to disk as a `.csv`, and pass the path to the stage. This is the default because it is easiest for users to inspect and reuse.
- For normal strategy files and group strategy files, `._set(strategyFileContent=...)` and `._set(groupStrategyFileContent=...)` may be shown only as alternate in-memory options after the disk-backed CSV example.
- Use stackable zero-shot NER models from `references/zero-shot-models.md` when the user wants configurable entity coverage for pixel PHI or metadata cleanTag workflows.
- Treat `templates.md` as a template index only. Copy code from the route-specific template file.

## NER Strategy Choice

Keep pipeline builder and zero-shot NER separate:

- Pipeline builder path: use Healthcare NLP `PretrainedPipeline(...)` from `clinical_pipeline` in `models.yaml`. Pass it as `pretrained_pipeline` to `dicom_pipeline_builder(...)`. Do not insert `PretrainedZeroShotNER` stages or stacked zero-shot helper functions into this builder.
- Pipeline builder replies must include the builder implementation itself, not only the final `pipeline = dicom_pipeline_builder(...)` block.
- Zero-shot stack path: use stage-level `PretrainedZeroShotNER().pretrained(...)` models from `models.yaml` `zero_shot_ner_models.options`, with usage patterns from `zero-shot-models.md`. Build a NER function that returns a fitted `PipelineModel`, then place that fitted model directly inside the outer `PipelineModel`.
- Do not pass a zero-shot model name to `dicom_pipeline_builder(...)` as `clinical_pipeline_name`.
- For `metadata_clean_tag_ner`, use `build_stacked_zero_shot_metadata_pipeline(input_text="tag_text")` from `template-zero-shot.md`. Do not use `build_stacked_zero_shot_ner_pipeline(...)` for metadata cleanTag.
- For DICOM pipeline builder workflows, do not use `build_stacked_zero_shot_ner_pipeline(...)`. The builder uses `dicom_pipeline_builder(...)` plus a Healthcare NLP `PretrainedPipeline(...)`.
- For pixel de-identification without the DICOM pipeline builder, including encapsulated PDF visible-text de-identification, use `build_stacked_zero_shot_ner_pipeline(input_text="text", output_chunk="merged_ner_chunk")` after OCR has produced `text`.
- Some `clinical_pipeline` names contain `zeroshot`, but they are still Healthcare NLP `PretrainedPipeline(...)` pipelines for the builder. They are not the same as stackable `PretrainedZeroShotNER` stages.
- If the user says "use the builder", use `clinical_pipeline` and do not read `zero-shot-models.md` unless they also ask for zero-shot/entity stacking.
- If the user says "zero-shot", "stack models", or asks for configurable entity coverage, use `zero-shot-models.md`; use the metadata helper for metadata cleanTag and the pixel helper for non-builder pixel PHI.
- If the user asks for a custom zero-shot hybrid, build it as direct custom stages in an outer `PipelineModel`; do not use `dicom_pipeline_builder(...)`.

## Pipeline Architecture Rules

- Outer DICOM, OCR, PDF, metadata, and Spark OCR workflows must use `PipelineModel(stages=[...])`.
- A pipeline should not be wrapped in a function unless the user explicitly asks, except Healthcare NLP/NER subpipelines.
- NER subpipelines must always be wrapped in a function.
- NER subpipeline functions must start with `DocumentAssembler`, end with `ChunkMergeApproach`, `DeIdentification`, or the final NLP stage needed by the downstream DICOM stage, fit on an empty DataFrame, and return the fitted `PipelineModel`.
- The first DICOM stage in generated metadata and pixel de-identification starter flows must consume `content`; encapsulated PDF uses the grounded PDF template.
- In pixel workflows, the input DataFrame should include both `content` and `path`: image extraction consumes `content`, while later path-based stages such as `DicomDrawRegions` consume `path`.
- Pixel PHI workflows using OCR text must output OCR text as `text`.
- `DicomToImageV3` output must be `image`.
- `DicomDrawRegions` output must be `dicom_pixel_cleaned`.
- `DicomMetadataDeidentifier` output must be `dicom_metadata_cleaned`.
- Original metadata output must be `metadata_original`; cleaned metadata output must be `metadata_cleaned`.
- Generated examples must define `dicom_path` and `dicom_df` before calling `.transform(...)`.
- Never drop `path`. If a DICOM stage consumes `path`, always use `setKeepInput(True)`.
- If a DICOM stage consumes `content`, use `setKeepInput(False)` unless a later stage still needs `content` in the same pipeline.
- If a DICOM stage creates a DICOM bytes output column, preserve that bytes column for display, saving, comparison, or downstream stages.
- Exception: in combined pixel plus metadata workflows, `DicomMetadataDeidentifier` consumes `["dicom_pixel_cleaned"]`, outputs `dicom_metadata_cleaned`, and may use `setKeepInput(False)` because `dicom_metadata_cleaned` replaces `dicom_pixel_cleaned` as the authoritative final bytes column.
- `DicomDrawRegions` always consumes `path`, because `DicomToImageV3` extracts the image from `content` and drops heavy content bytes.
- Treat `DicomDrawRegions` as an aggregation stage. After it runs, assume upstream intermediate columns such as `image`, `text`, `regions`, and `coordinates` are gone.
- If a result DataFrame comes directly after `DicomDrawRegions`, validate it with `display_dicom` on the DICOM output column; do not use `.select(...)` for upstream intermediate columns.
- If stages run after `DicomDrawRegions`, such as `DicomMetadataDeidentifier` or `DicomToMetadata`, their outputs can be present and may be selected or displayed.

## Formatting Rules

- Use backslash chaining for Spark OCR and Healthcare NLP stages.
- Keep every `.pretrained(...)` call on one line with model name, language, and remote path.
- Do not split the class and `.pretrained(...)` across lines.
- Strict rule: `.setInputCols(...)` must always be written on a single line. Never span `.setInputCols([ ... ])` across multiple lines, even when there are multiple input columns.
- Keep short setters, enum values, scalar params, and short helper calls on one line.
- Define long labels, prompts, entity lists, and config values as named variables.
- Avoid wrapping stage definitions, short variables, short strings, or short function calls in parentheses.
- If a line can stay on one line without hurting readability or execution, keep it on one line.

Bad pretrained formatting:

```python
vlm_ocr = MedicalVisionLLM \
    .pretrained(
        "jsl-ocr-gguf-vlm1",
        "en",
        "clinical/ocr",
    ) \
    .setInputCols([
        "caption_document",
        "image_assembler",
    ]) \
    .setOutputCol("completions")
```

This is bad because `.pretrained(...)` is split across lines and `.setInputCols(...)` spans multiple lines.

Good pretrained formatting:

```python
vlm_ocr = MedicalVisionLLM.pretrained("jsl-ocr-gguf-vlm1", "en", "clinical/ocr") \
    .setInputCols(["caption_document", "image_assembler"]) \
    .setOutputCol("completions") \
    .setNGpuLayers(99) \
    .setNCtx(32768) \
    .setNParallel(1) \
    .setNBatch(2048) \
    .setNUbatch(1024) \
    .setNPredict(1024) \
    .setTemperature(0.01) \
    .setTopK(1) \
    .setTopP(1.0) \
    .setRepeatPenalty(1.03) \
    .setRepeatLastN(256) \
    .setMinKeep(0) \
    .setNProbs(0) \
    .setBatchSize(1) \
    .setDisableLog(False)
```

## Standard Imports

Use this exact import block for all `sparknlp`, `sparknlp_jsl`, and `sparkocr` stage, annotator, transformer, utility, enum, and schema imports. Do not invent direct imports such as `from sparkocr.schemas import dicomExternalSchema`; this block already exposes Spark OCR schemas.

```python
from sparknlp.annotator import *
from sparknlp.base import *

import sparknlp_jsl
from sparknlp_jsl.annotator import *

import sparkocr
from sparkocr.transformers import *
from sparkocr.utils import *
from sparkocr.enums import *
from sparkocr.schemas import *
```

Use `from sparknlp.pretrained import PretrainedPipeline` for Healthcare NLP `clinical_...` pretrained pipelines.

Use `from sparkocr.pretrained import PretrainedPipeline` for Spark OCR DICOM, PDF, and image pretrained pipelines.

## Final Code Formatting Gate

Before replying with code, enforce this as a mandatory final rule. This section overrides normal formatter preferences for generated examples in this skill.

- Generated code must not span multiple lines unless it is absolutely necessary for correctness, a long dictionary, a long entity-label list, a long pipeline stage list, or a genuinely long string.
- Keep `.pretrained(...)`, short setters, scalar params, enum params, function calls, variable assignments, and short lists on one line.
- `.setInputCols(...)` is never allowed to span multiple lines; keep it on one line in every generated example.
- Name the main configuration dictionary `config` in every generated pipeline. Do not use route-specific names such as `blanket_redaction_config`.
- If a multi-line block is necessary, keep each chained setter readable and avoid splitting a single method call across multiple lines.
- Do one final pass before answering and collapse any avoidable multi-line code into a single line.

Negative examples:

```python
vlm_ocr = MedicalVisionLLM \
    .pretrained(
        "jsl-ocr-gguf-vlm1",
        "en",
        "clinical/ocr",
    ) \
    .setInputCols([
        "caption_document",
        "image_assembler",
    ]) \
    .setOutputCol("completions")
```

```python
position_finder = PositionFinder() \
    .setInputCols([
        "chunk",
    ]) \
    .setOutputCol(
        "coordinates",
    )
```

This is bad because `.setInputCols(...)` and `.setOutputCol(...)` span multiple lines.

```python
phi_detection_model = build_phi_detection_pipeline(
    input_text="text",
    output_chunk="merged_ner_chunk",
)
```

Positive examples:

```python
vlm_ocr = MedicalVisionLLM.pretrained("jsl-ocr-gguf-vlm1", "en", "clinical/ocr") \
    .setInputCols(["caption_document", "image_assembler"]) \
    .setOutputCol("completions") \
    .setNGpuLayers(99) \
    .setNCtx(32768) \
    .setNParallel(1) \
    .setNBatch(2048) \
    .setNUbatch(1024) \
    .setNPredict(1024) \
    .setTemperature(0.01) \
    .setTopK(1) \
    .setTopP(1.0) \
    .setRepeatPenalty(1.03) \
    .setRepeatLastN(256) \
    .setMinKeep(0) \
    .setNProbs(0) \
    .setBatchSize(1) \
    .setDisableLog(False)
```

```python
position_finder = PositionFinder() \
    .setInputCols(["chunk"]) \
    .setOutputCol("coordinates")
```

```python
phi_detection_model = build_phi_detection_pipeline(input_text="text", output_chunk="merged_ner_chunk")
```
