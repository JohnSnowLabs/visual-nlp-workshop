# Redaction Stages

Use this file for shared visible text and PHI redaction routing rules.

- PHI-only redaction uses the common pixel builder by default. It requires OCR text, NER chunks, coordinates, and an input-specific draw/redaction stage.
- Remove-all visible text uses detected text regions directly and skips OCR recognition and NER. It is currently routed only for DICOM.
- DICOM pixel routes use `DicomDrawRegions`.
- Image and PDF routes currently support PHI-only redaction, not blanket visible-text removal.
- Do not assume output columns survive aggregation/redraw stages; keep inspection pipelines separate when users need text, regions, or coordinates.
