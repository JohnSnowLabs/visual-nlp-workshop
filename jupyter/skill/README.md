# Visual NLP Skill

An LLM skill for building John Snow Labs **Visual NLP** workflows — OCR, PHI redaction, and metadata de-identification — for **DICOM**, **image**, and **PDF** inputs. It generates copy-ready Spark OCR / Healthcare NLP notebook code; it does not run Spark itself. It follows the emerging `SKILL.md` convention (YAML frontmatter + Markdown instructions) and works with any LLM agent host capable of loading such a skill and reading the reference files it points to.

## Using the skill

Just start talking about a Visual NLP task, or use one of the slash commands below. The skill always shows a menu first — it never guesses a workflow from your prompt and never generates code before you pick a task by number or exact name.

| Command | Shows |
|---|---|
| `/all_actions` | Action categories |
| `/all_tasks` | All 14 tasks |
| `/dicom_tasks` | DICOM-only tasks (1–10) |
| `/image_tasks` | Image-only tasks (1–2) |
| `/pdf_tasks` | PDF-only tasks (1–2) |
| `/input_types` | Supported input types (DICOM, Image, PDF) |

Once you pick a task — by number ("5") or by name ("DICOM pixel PHI redaction with the common pixel builder") — the skill reads only the reference files that task needs and generates the workflow.

## Available tasks

**DICOM**
1. Metadata inspection — extract metadata, no changes made.
2. Metadata de-identification — hash/remove/replace/shift tags via a strategy file.
3. Metadata strategy file creation — build a copy-ready strategy CSV without a full pipeline.
4. Free-text metadata de-identification — `cleanTag` + stackable zero-shot NER.
5. Pixel PHI redaction, common pixel builder — configurable OCR (VLM/V2/V3) + clinical pipeline.
6. Pixel PHI redaction, zero-shot NER — VLM/V2/V3 OCR + stackable zero-shot NER.
7. Remove-all visible text — blanket redaction, no OCR/NER.
8. Encapsulated PDF de-identification, common pixel builder — VLM/V1/V2/V3 OCR + clinical pipeline.
9. Encapsulated PDF de-identification, zero-shot NER — VLM/V1/V2/V3 OCR + stackable zero-shot NER.
10. Pretrained baseline — single-call Spark OCR pipeline (minimal / pseudonym / full anonymization).

**Image**

11. PHI redaction, common pixel builder — configurable OCR (VLM/V1/V2/V3) + clinical pipeline.
12. PHI redaction, zero-shot NER — configurable OCR + stackable zero-shot NER.

**PDF**

13. PHI redaction, common pixel builder — configurable OCR (VLM/V1/V2/V3) + clinical pipeline.
14. PHI redaction, zero-shot NER — configurable OCR + stackable zero-shot NER.

Every task's exact wording lives in `SKILL.md`; this list is a summary, not the source of truth.

## Repository structure

```
SKILL.md                                  Entry point: menu text, routing/read-order rules, policies
references/
  routing.yaml                            Maps each task to its route: input type, template, read list, models, checkpoints
  models.yaml                             Model/engine catalog + selection guides (cost/accuracy, hardware defaults)
  checkpoints.yaml                        Machine-checkable correctness rules per route

  common/                                 Shared across DICOM, image, and PDF
    formatting.md                         Code style rules (backslash chaining, one-line setters, block ordering)
    imports.md                            Standard import block
    utilities.md                          Display/save/metadata-comparison helper functions
    stages-ocr.md                         MedicalVisionLLM (VLM), ImageToText/V2/V3 stage definitions
    stages-text-detection.md              ImageTextDetector / ImageTextDetectorV2
    stages-ner.md                         Healthcare NLP NER stage notes
    stages-redaction.md                   Redaction stage notes
    visual_stage.md                       PositionFinder, ImageDrawRegions
    zero-shot-models.md                   Zero-shot NER usage rules
    template-pixel-builder-common.md      Shared `nlp_builder` helper (extracts NER chunk col from a clinical pipeline)
    template-pixel-builder-image-pdf.md   Shared image/PDF common-pixel-builder implementation (VLM/V1/V2/V3)
    template-zero-shot-builder.md         Shared zero-shot NER stacking builder + config
    template-zero-shot-image-pdf.md       Shared image/PDF zero-shot OCR ingestion (VLM/V1/V2/V3)

  dicom/
    stages.md                             DICOM-specific stage notes (DicomToImageV3, DicomDrawRegions, etc.)
    strategy-files.md                     Metadata action catalog, VR compatibility, mapping policy (used by metadata_deid, metadata_clean_tag_ner)
    metadata-creation.md                  Canonical strategy-CSV creation code pattern
    template-metadata.md                  metadata_inspection, metadata_deid routes
    template-strategy-file.md             metadata_strategy_file route — self-contained, isolated
    template-zero-shot.md                 pixel_phi_zero_shot, metadata_clean_tag_ner routes
    template-pixel-builder.md             pixel_phi_builder route (VLM/V2/V3, no V1)
    template-pixel-blanket.md             pixel_remove_all_text route
    template-encapsulated-pdf.md          encapsulated_pdf_phi_vlm, encapsulated_pdf_phi_builder routes (V1 allowed)
    template-pretrained.md                pretrained_baseline route

  image/
    stages.md                             Image-specific stage notes
    template-phi-zero-shot.md             image.pixel_phi_zero_shot route

  pdf/
    stages.md                             PDF-specific stage notes
    template-phi-zero-shot.md             pdf.pixel_phi_zero_shot route
```

## For maintainers

- **Adding a task**: add a route to `routing.yaml` (input type, template file/section, read list, models, checkpoints), add its menu line(s) to `SKILL.md`, and write the template content. If the template file will hold more than one route's section, set `template_section` (or `template_sections`) on every route pointing at it — omitting it means the whole file is in scope, which has caused real bugs in this skill before.
- **Config-driven builders**: every route with an OCR-engine or detector choice follows the same shape — a `*_config_cleaner`, `*_required_config_keys`, and `*_config_checker` function, gating optional keys (`detector_engine`, `gpu`, `nPredict`, …) behind the branch that actually needs them. Before merging a new one, extract it and fuzz-test every engine combination with real Python execution (`compile()` + `exec()`), not just a syntax check — several real `KeyError` bugs were only caught this way.
- **Duplication**: most content should live in exactly one file, read by every route that needs it. The two deliberate exceptions are `dicom/strategy-files.md` ↔ `dicom/template-strategy-file.md` (isolation) and `dicom/metadata-creation.md` ↔ the inline example in `dicom/template-metadata.md` (concrete example + canonical pattern). Anything else that looks duplicated is probably a bug, not a design choice.
- **Verifying integrity** after any change: confirm every route's `read` list resolves to a real file, every `checkpoints:`/`models:` entry resolves in `checkpoints.yaml`/`models.yaml` with no orphans on either side, and every multi-section template file has `template_section` set on all its routes.
