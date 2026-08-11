# FAQ

Use this FAQ when the user asks what DICOM operations are available, which workflow to choose, or how the main de-identification paths differ. Keep answers short and route the user to the matching pipeline template.

## Getting Started

### What DICOM workflows can you help me build?

I can help with metadata inspection, metadata de-identification, free-text metadata de-identification for fields such as Study Comments or Series Description, burned-in pixel PHI redaction, aggressive visible-text removal, encapsulated PDF de-identification, and Spark OCR pretrained DICOM baselines.

### Which workflow should I choose first?

For pixel PHI or pixel plus metadata de-identification, start with `pixel_phi_builder`. For quick Spark OCR baselines, start with `pretrained_dicom_baseline`. Metadata inspection and deterministic metadata-only de-identification do not need GPU or pretrained models.

### Should I use the pipeline builder or hand-built stages?

Use the configurable DICOM pipeline builder first for pixel PHI or pixel plus metadata workflows. Use hand-built stages when the user asks for a specific detector/OCR path, metadata-only workflow, blanket redaction, or encapsulated PDF workflow.

When using the builder, output the complete builder code, including helper functions, config, model loading, transform call, and inspection pass when relevant. Do not output only the `dicom_pipeline_builder(...)` call-site.

### Are pipeline builder models and zero-shot NER models the same thing?

No. Pipeline builder models are Healthcare NLP `PretrainedPipeline(...)` pipelines from `clinical_pipeline`. Zero-shot NER models are stage-level `PretrainedZeroShotNER().pretrained(...)` models that can be stacked in a custom NER function.

### When should I ask the user a question?

Ask only when the requested surface or output cannot be safely inferred. Do not ask hardware by default; choose pretrained/GPU and PHI-only redaction unless the user asks for CPU, cheaper OCR, custom stages, or no pretrained models.

## Metadata Inspection

### How do I inspect DICOM metadata without changing the DICOM?

Use `metadata_inspection`: `DicomToMetadata` outputs `metadata_original`.

### Should metadata inspection use pretrained models?

No. Metadata inspection does not need GPU or pretrained models.

### What output column should metadata inspection use?

Use `metadata_original`.

## Metadata De-Identification

### How do I de-identify structured DICOM tags?

Use `metadata_deid`: extract `metadata_original`, run `DicomMetadataDeidentifier`, then extract `metadata_cleaned`.

### How do I compare original and cleaned metadata?

Use `build_metadata_comparison_df(result)` after the pipeline outputs `metadata_original` and `metadata_cleaned`. It returns a pandas DataFrame with `Tag`, `VR`, `Original_Value`, `Cleaned_Value`, `Is_Changed`, and `Is_Deleted`.

### How do I inspect one metadata column?

Use `build_metadata_df(result, metadata_col="metadata_original")` for single metadata inspection. Use `build_metadata_comparison_df(result)` only when both original and cleaned metadata columns exist.

### Which metadata actions are available?

Show the full action catalog from `strategy-files.md` before giving examples. Include `cleanTag`, `remove`, `delete`, `hashId`, `patientHashId`, `replaceWithLiteral`, `replaceWithMapping`, `replaceWithRandomName`, `shiftDateByFixedNbOfDays`, `shiftDateByRandomNbOfDays`, `shiftTimeByRandom`, `shiftUnixTimeStampRandom`, `shiftAgeByRandom`, and `capAgeAt99IfOver90`.

### How should I create strategy files?

Create strategy CSV content with `textwrap.dedent`, write it to disk as `.csv`, and pass the file path to the DICOM stage. Use this default for normal strategy files, group strategy files, `cleanTag`, and `replaceWithMapping`. Show `._set(strategyFileContent=...)` or `._set(groupStrategyFileContent=...)` only as alternate in-memory options.

If the user provides `Tags`, `VR`, and `Name`, map `Action` and `Option` for them using `template-strategy-file.md`. The input can be messy or come as text, tables, files, DataFrame previews, or notebook output, so normalize it first instead of enforcing a hard input format. Preserve their tag, VR, and name exactly in the final CSV. If any row is ambiguous or the model is unsure, ask a focused question instead of guessing.

### What is the difference between tag strategy and group strategy?

Tag strategy targets specific tags. Group strategy targets all tags in a DICOM group using group rows such as `"(0020,)"` with action `remove` or `delete`.

### When do I use `replaceWithMapping`?

Use `replaceWithMapping` when values should come from external mapping data. It is the only action that uses explicit `Nested` in `Option`. Always build the mapping DataFrame with `dicomExternalSchema` from `sparkocr.schemas`, use the default `external_mapping` column unless the user asks otherwise, write DICOM tag keys without parentheses or commas, and make sure the path values match exactly between the DICOM DataFrame and external mapping DataFrame before joining.

### When should `DicomMetadataDeidentifier` consume `path`, `content`, or `dicom_pixel_cleaned`?

Use `path` for metadata-only and cleanTag starter workflows. Use `dicom_pixel_cleaned` when metadata de-identification follows pixel redaction.

### If input is `path`, should I keep input?

Yes. Never drop `path`. If a DICOM stage consumes `path`, use `setKeepInput(True)`.

## Free-Text Metadata De-Identification

### When do I need free-text metadata de-identification?

Use `metadata_clean_tag_ner` when free-text metadata fields such as study descriptions, protocol names, or comments may contain PHI.

### What columns are required for cleanTag?

Use `tag_text`, `tag_mapping`, `deid_documents`, and `dicom_metadata_cleaned`.

### What should the NLP output column be for metadata cleanTag?

Use `deid_documents`; `DicomMetadataDeidentifier` should set `setTagCleanedCol("deid_documents")`.

### Can I use zero-shot NER models for metadata free text?

Yes. Use `models.yaml` for the zero-shot model catalog and `zero-shot-models.md` for usage rules. The final metadata cleanTag output must still be `deid_documents`.

### Which stacked zero-shot helper should metadata cleanTag use?

Use `build_stacked_zero_shot_metadata_pipeline(input_text="tag_text")` for `metadata_clean_tag_ner`. Use `build_stacked_zero_shot_ner_pipeline(...)` for pixel or encapsulated PDF de-identification without the DICOM pipeline builder.

## Pixel PHI Redaction

### How do I remove burned-in PHI from DICOM images?

Use `pixel_phi_builder` by default. It uses `DicomToImageV3`, VLM or OCR, Healthcare NLP, `PositionFinder`, `DicomDrawRegions`, and optional metadata de-identification.

### What OCR path should I use by default?

Use VLM OCR on GPU for highest accuracy unless the user asks for cheaper CPU/GPU paths.

### How should image extraction be configured for VLM?

When VLM is used for OCR, use `setCompressImage(True)` and write dimensions to `frame_dims`: `BinaryToImage` and `PdfToImage` use `setImageDimsCol("frame_dims")`; `DicomToImageV3` uses `setFrameDimsCol("frame_dims")`. When VLM is not used, set `setCompressImage(False)` on image extraction.

### What are the cheaper OCR options?

Use V2 for CPU/GPU printed OCR. Use V3 when the user asks for CPU-only OCR.

### What columns should pixel PHI workflows produce?

Use `image` for `DicomToImageV3`, `text` for OCR text, `coordinates` for redaction coordinates, and `dicom_pixel_cleaned` for pixel-cleaned DICOM bytes.

### Can I select OCR text or coordinates after `DicomDrawRegions`?

No. `DicomDrawRegions` is an aggregation stage, so assume upstream intermediate columns are gone after it runs. If `DicomDrawRegions` is the final stage, use `display_dicom` on `dicom_pixel_cleaned`. If later stages run after it, such as `DicomMetadataDeidentifier` or `DicomToMetadata`, their new output columns can be selected.

### What should `PositionFinder` consume?

It should consume only the final chunk column, not `text`.

### Can I stack zero-shot NER models for pixel PHI?

Yes. Use `models.yaml` to select models and `zero-shot-models.md` for stacking rules. Merge chunks into `merged_ner_chunk`, and pass only that chunk column to `PositionFinder`.

## Aggressive Visible-Text Removal

### How do I remove all visible text instead of PHI-only text?

Use `pixel_remove_all_text`. It detects text regions and passes them directly to `DicomDrawRegions`; no OCR or NER is required.

### Can blanket redaction use ImageTextDetectorV2?

Yes. `pixel_remove_all_text` can use `ImageTextDetector` or `ImageTextDetectorV2`. Default to `ImageTextDetector.pretrained(...)`; switch to `ImageTextDetectorV2.pretrained(...)` when requested.

### Why is this path resource friendly?

It does not run OCR or NER. It only detects text regions and redacts them.

## Encapsulated PDF

### How do I de-identify an encapsulated PDF inside DICOM?

Use `encapsulated_pdf_phi_vlm`: `DicomToPdf -> PdfToImage -> VLM OCR -> build_stacked_zero_shot_ner_pipeline -> PositionFinder -> ImageDrawRegions -> ImageToPdf -> DicomUpdatePdf`.

### Should encapsulated PDF redaction use DicomDrawRegions?

No. Use `ImageDrawRegions` for PDF page images, then rebuild the PDF and update the DICOM.

### Why keep an intermediate result for PDF workflows?

PDF reconstruction can drop intermediate inspection columns. Keep an intermediate DataFrame when the user wants to inspect pages, OCR text, regions, or coordinates.

## Pretrained DICOM Baselines

### What is the easiest pretrained baseline?

Use `dicom_deid_generic_augmented_minimal` for least-intrusive DICOM de-identification.

### When should I use full anonymization?

Use `dicom_deid_full_anonymization` when all visible text and most metadata should be removed or anonymized for public sharing, research, or compliance.

### When should I use pseudonymization?

Use `dicom_deid_generic_augmented_pseudonym` when metadata structure should be preserved with randomized or pseudonymized identifiers.

## Display And Output

### How should I display DICOM results?

Use `display_dicom(df=result, fields="<final_dicom_col>", limit=1, width=300)`.

### Should I save DICOM output to disk?

Yes, include the simple `save_dicom_to_disk(...)` utility function body before calling it for every final DICOM-producing workflow except metadata inspection. It should only create the save directory if needed, extract DICOM bytes, keep the same base filename from `path`, save to disk, and return saved paths.

### When should I use display_images?

Use `display_images` only when the user explicitly wants to inspect image content.

### What final DICOM output columns should I expect?

Use `dicom_metadata_cleaned` for metadata-cleaned output, `dicom_pixel_cleaned` for pixel-cleaned output, and `dicom` for encapsulated PDF or pretrained baseline outputs.
