# Zero-Shot NER Models

Use this file when the user asks for zero-shot PHI detection, model/entity coverage, stacked NER models, or configurable NER for pixel or metadata de-identification.

The model-card links in this file are external John Snow Labs sources. When presenting model-card information to users, say it came from external JSL model cards or docs and that external information can change or may not be 100% correct for every installed version.

## Table Of Contents

- Model selection rules
- Stage usage pattern
- Stacking pattern
- Pixel PHI usage
- Metadata cleanTag usage
- Model catalog location

## Model Selection Rules

- Use these models for both pixel OCR text and metadata free-text de-identification.
- Read `models.yaml` `zero_shot_ner_models.options` for model names, one-line load code, model-card links, and entity lists.
- Use stackable stage-level `PretrainedZeroShotNER` models, not `PretrainedPipeline`, when multiple zero-shot models must run together.
- Do not pass these model names to common pixel builder routes as clinical pipeline names.
- Do not mix this zero-shot stack with the builder path. If the user asks for a custom zero-shot hybrid, build direct custom stages in an outer `PipelineModel` instead of using the common pixel builder.
- If the user asks for the default builder, use `clinical_pipeline` from `models.yaml`, not this file.
- For DICOM metadata cleanTag, use the DICOM-specific builder in `dicom/template-zero-shot.md`.
- For pixel, image, standalone PDF, or DICOM encapsulated PDF OCR text de-identification without the common pixel builder, use `common/template-zero-shot-builder.md`.
- Default to `zeroshot_ner_deid_subentity_merged_medium` for balanced coverage unless the user asks for a specific model.
- Use `large` models when the user asks for higher recall or accuracy and has enough resources.
- Use `generic` models when broad PHI classes are enough.
- Use `subentity` models when the user wants specific entity labels such as `DOCTOR`, `PATIENT`, `CITY`, `ZIP`, or `MEDICALRECORD`.
- Use `nonMedical` models when the text is not clinical prose or the user explicitly asks for non-medical coverage.
- Keep model labels as separate variables before stage definitions.
- Keep every `.pretrained(...)` call on one line.

## Stage Usage Pattern

Model cards use this general pattern. Start with a copy-ready model and swap the model name/labels from `models.yaml` when needed:

```python
subentity_merged_medium_labels = [
    "DOCTOR",
    "PATIENT",
    "AGE",
    "DATE",
    "HOSPITAL",
    "CITY",
    "STREET",
    "STATE",
    "COUNTRY",
    "PHONE",
    "IDNUM",
    "EMAIL",
    "ZIP",
    "ORGANIZATION",
    "PROFESSION",
    "USERNAME",
]

zero_shot_ner = PretrainedZeroShotNER().pretrained("zeroshot_ner_deid_subentity_merged_medium", "en", "clinical/models") \
    .setInputCols(["sentence", "token"]) \
    .setOutputCol("zero_shot_ner") \
    .setPredictionThreshold(0.5) \
    .setLabels(subentity_merged_medium_labels)
```

Always place `PretrainedZeroShotNER().pretrained(...)` on one line. Do not split `PretrainedZeroShotNER()` and `.pretrained(...)` across lines.

## Stacking Pattern

Use `common/template-zero-shot-builder.md` when the user wants to stack multiple zero-shot NER models for OCR text without the common pixel builder. Each model gets its own NER output and chunk output, and the common builder merges chunks into `merged_ner_chunk`.

## Pixel PHI Usage

For pixel OCR text, place the returned stacked NER `PipelineModel` inside the outer `PipelineModel` after OCR has produced `text`. Keep input-specific rendering, drawing, reconstruction, display, and save code in the DICOM, image, or PDF template selected by routing.

```python
pipeline = PipelineModel(stages=[
    input_to_image,
    ocr_stages,
    build_stacked_zero_shot_ner_pipeline(input_text="text", output_chunk="merged_ner_chunk"),
    position_finder,
    draw_regions
])
```

`PositionFinder` must consume only the merged chunk column. Use `common_zero_shot_position_finder` from `common/visual_stage.md`.

## Metadata cleanTag Usage

DICOM metadata cleanTag is DICOM-specific. Keep its strategy files, `DicomToMetadata`, `DicomMetadataDeidentifier`, and `build_stacked_zero_shot_metadata_pipeline(...)` code in `dicom/template-zero-shot.md`.

## Model Catalog Location

The zero-shot model catalog lives in `models.yaml` under `zero_shot_ner_models.options`. Keep model names, one-line load code, model-card links, and entity lists there. Keep this file focused on usage patterns.
