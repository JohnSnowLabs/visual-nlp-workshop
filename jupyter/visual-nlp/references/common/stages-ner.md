# NER Stages

Use this file for NER wiring shared by image, PDF, and non-builder DICOM routes.

## Default Pixel Builder NER

- Use Healthcare NLP `PretrainedPipeline(...)` with common pixel builder routes for DICOM, image, and PDF inputs.
- Use `nlp_builder(spark_session, pretrained_pipeline)` from `common/template-pixel-builder-common.md` `common_pixel_builder_helpers` for default Pixel Builder routes.

```python
ner_output, nlp_pipeline = nlp_builder(spark_session, pretrained_pipeline)
```

## Zero-Shot NER

- Use stage-level `PretrainedZeroShotNER().pretrained(...)` for configurable image, PDF, metadata cleanTag, and non-builder pixel PHI routes.
- Keep each `PretrainedZeroShotNER().pretrained(...)` call on one line.
- Wrap NER subpipelines in functions that fit on an empty DataFrame and return a fitted `PipelineModel`.
- Read `common/zero-shot-models.md` before generating zero-shot model code.
- Use `common/template-zero-shot-builder.md` for OCR-text zero-shot NER shared by DICOM pixel, DICOM encapsulated PDF, image, and PDF routes.

```python
phi_detection_model = build_stacked_zero_shot_ner_pipeline(input_text="text", output_chunk="merged_ner_chunk")
```

Use `common/visual_stage.md` for `PositionFinder` and coordinate-to-region stage contracts.
