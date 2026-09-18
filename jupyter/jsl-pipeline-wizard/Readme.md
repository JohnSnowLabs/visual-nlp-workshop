# DICOM UI

A browser-based configuration wizard for building John Snow Labs Visual NLP
de-identification pipelines. Open index.html in a browser, choose a workflow,
answer each step, and get copy-ready Python notebook code — no backend or
build step required.

## Workflows

- Strategy File Builder — Create a strategy CSV mapping DICOM tags to actions
  (hash, remove, replace, shift dates, etc.)
- Metadata De-id — De-identify structured DICOM header tags using a strategy
  file, with optional free-text NER
- Blanket Pixel De-id — Remove all visible text from DICOM pixel data;
  fastest pixel option, no OCR or NER
- PHI-Only Pixel De-id — Detect and redact only PHI in DICOM pixels using
  OCR and a clinical NLP pipeline
- Encapsulated PDF De-id — Redact PHI in PDFs stored inside DICOM files
  using OCR and a clinical pipeline

## Project Layout

- index.html     — app shell
- app.js         — state management and rendering
- styles.css     — design system
- workflows/     — one file per workflow; shared helpers in common.js