# Template Index

Do not copy pipeline code from this file. Use `routing.yaml` to select a pipeline, then open that pipeline's `template_file`.

| Template file | Sections |
|---|---|
| `template-metadata.md` | `metadata_inspection`, `metadata_deid`, `metadata_clean_tag_medical_ner_legacy` |
| `template-strategy-file.md` | `metadata_strategy_file` |
| `template-zero-shot.md` | `metadata_clean_tag_ner`, `pixel_phi_zero_shot` |
| `template-pixel-builder.md` | `pixel_phi_builder` |
| `template-pixel-blanket.md` | `pixel_remove_all_text` |
| `template-encapsulated-pdf.md` | `encapsulated_pdf_phi_vlm` |
| `template-pretrained.md` | `pretrained_dicom_baseline` |

Rules:
- Read `stages.md` before using a template.
- Read `models.yaml` before selecting a model.
- Read `zero-shot-models.md` with `template-zero-shot.md`.
- Read `strategy-files.md` for metadata strategy examples.
