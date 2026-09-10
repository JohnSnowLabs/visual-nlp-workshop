# DICOM Model Selector

## Supported Input Types

- **Standard DICOM Pixels** — conventional pixel data embedded in DICOM files
- **Encapsulated PDF** — DICOM files containing embedded PDF documents

---

## De-Identification Strategies for Standard DICOM Pixels

### 1. Blanket De-Identification

Removes **all visible text** from the pixel data, regardless of whether it is PHI.

**Pipeline:**

```mermaid
flowchart LR
    A[DICOM] --> B[Images]
    B --> C[Detect Regions]
    C --> D[Pixel Redaction]
    D --> E[Metadata Handling]
    E --> F[Final DICOM]
```
All stages are rule-based except region detection, which requires a text detector model.

**Available Text Detector Models:**

| Model | Hardware | Engine |
|---|---|---|
| `ImageTextDetectorV2` | CPU / GPU | PyTorch |
| `ImageTextDetector` | CPU / GPU | ONNX / OpenVINO |

---

### 2. PHI-Only De-Identification

Removes **only PHI text** from the pixel data using OCR and NER.

```mermaid
flowchart LR
    A[DICOM] --> B[Images]
    B --> C[Detect Regions\n+ Extract Text]
    C --> D[NER]
    D --> E[Generate Coordinates]
    E --> F[Pixel Redaction]
    F --> G[Metadata Handling]
    G --> H[Final DICOM]
```
  
This strategy adds OCR extraction, NER detection, and coordinate generation steps.

Two model combinations are supported:

- **External text detector + OCR model** — text detection and extraction handled by separate models
- **VLM (Vision Language Model)** — detection and extraction in a single pass (e.g., `MedicalVisionLLM`)

**Available OCR Models:**

| Model | Hardware | Engine | External Text Detector
|---|---|---|---|
| `ImageToTextV2` | CPU / GPU | ONNX | Required |
| `ImageToTextV3` | CPU | Tesseract | Required |
| `MedicalVisionLLM` | CPU / GPU | LlamaCpp | Not required |

> [!IMPORTANT]
> Any text detector model (`ImageTextDetectorV2` or `ImageTextDetector`) can be paired with `ImageToTextV2` or `ImageToTextV3`.

---

## De-Identification Strategies for Encapsulated DICOM PDF

Model selection for Encapsulated PDF DICOMs follows the same logic as PDF and Image de-identification — refer to this section for guidance on model selection for PDF/Image input types. 

```mermaid
flowchart LR
    A[DICOM] --> B[Pdf]
    B --> C[Images]
    C --> D[Detect Regions\n+ Extract Text]
    D --> E[NER]
    E --> F[Generate Coordinates]
    F --> G[Pixel Redaction]
    G --> H[Regenerate PDF]
    H --> I[Regenerate DICOM]
    I --> J[Metadata Handling]
    J --> K[Final DICOM]
```

| Model | Hardware | Engine | External Text Detector |
|---|---|---|---|
| `ImageToText` | CPU | Tesseract | Required |
| `ImageToTextV2` | CPU / GPU | ONNX | Required |
| `ImageToTextV3` | CPU | Tesseract | Required |
| `MedicalVisionLLM` | CPU / GPU | LlamaCpp | Not required |

> [!IMPORTANT]
> `ImageToText` is supported **only** for Image/PDF/Encapsulated PDF DICOMs. It is not recommended for standard DICOM Pixels.
