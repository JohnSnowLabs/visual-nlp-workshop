const workflowEncapsulatedPdf = {
    name: "DICOM PHI-Only PDF De-id",
    description: "Use OCR + Healthcare NLP clinical pipeline to detect and redact only PHI regions.",
    steps: [
    // ── Step 0: Overview ──────────────────────────────────────────────────────
    {
      title: "Overview",
      kind: "Overview",
      description: "What this workflow does and when to use it.",
      output: "",
      mustUnderstand: [],
      controls: [
        {
          type: "workflow-overview",
          key: "wf_overview",
          label: "Workflow summary",
          required: false,
        data: {
          what: [
          "Detects and redacts PHI in PDFs encapsulated inside DICOM files.",
          "Uses OCR + Healthcare NLP clinical pipeline.",
          ],
          outputs: [
          "DICOM files with the encapsulated PDF pages de-identified.",
          ],
          prerequisites: [
          "DICOM files that contain an encapsulated PDF.",
          "A GPU or CPU cluster depending on the chosen OCR engine.",
          ],
          whenToUse: [
          "When your DICOM files wrap a PDF (e.g. structured reports, consent forms).",
          ],
        },
        },
      ],
      decisionSummary: () => [],
      code: () => "",
    },


      {
        title: "Hardware Options",
        kind: "Required",
        description: "Select appropriate hardware.",
        output: "Determines the hardware option for inference.",
        mustUnderstand: [
          "VLM OCR (MedicalVisionLLM) requires a GPU — selecting VLM locks hardware to GPU.",
          "Tesseract is CPU-native and requires no model download.",
          "Non-VLM OCR (ImageToTextV2/V3) and Text Detection use setUseGPU() and support both GPU and CPU.",
          "Healthcare NLP (clinical pipeline) does not use setUseGPU() — it inherits from the Spark session.",
        ],
        controls: [
          {
            type: "choice", key: "px_ocrType", label: "OCR type",
            value: "vlm", required: true,
            options: [
              { label: "VLM OCR",     value: "vlm",       summary: "MedicalVisionLLM — highest accuracy. Requires GPU." },
              { label: "Non-VLM OCR", value: "non-vlm",   summary: "ImageToTextV2 or V3 — lighter, CPU-compatible." },
              { label: "Tesseract",   value: "tesseract", summary: "Open-source Tesseract engine — CPU-native, no model download required." },
            ],
            help: "VLM requires a GPU. Non-VLM and Tesseract support both GPU and CPU.",
            impact: (v) => v === "vlm" ? "VLM selected — GPU required, Text Detection hidden." : v === "tesseract" ? "Tesseract selected — CPU-native, Text Detection hidden." : "Non-VLM selected — Text Detection available, CPU supported.",
            onSelect: (v, settings) => {
              if (v === "vlm") settings.useGPU = true;
              if (v === "non-vlm") settings.px_ocrModel = "ImageToTextV2";
              if (v === "tesseract") { settings.px_ocrModel = "ImageToText"; settings.useGPU = false; }
            },
          },
          {
            type: "choice", key: "useGPU", label: "Hardware",
            required: true,
            options: (s) => {
              const gpu = { label: "GPU", value: true,  summary: "Run on GPU. Required for VLM OCR. Fastest option." };
              const cpu = { label: "CPU", value: false, summary: "Run on CPU. Required for Tesseract. Also supported by Non-VLM OCR." };
              return s.px_ocrType === "vlm" ? [gpu] : s.px_ocrType === "tesseract" ? [cpu] : [gpu, cpu];
            },
            help: "VLM requires GPU. Tesseract is CPU-only. Non-VLM supports both.",
            impact: (v) => v ? "Models will use GPU acceleration (setUseGPU(True))." : "Models will run on CPU (setUseGPU(False)).",
          },
        ],
        decisionSummary: (s) => [
          `OCR type: ${s.px_ocrType === "vlm" ? "VLM OCR" : "Non-VLM OCR"}.`,
          `Hardware: ${s.useGPU ? "GPU" : "CPU"}.`,
        ],
        code: (s) => `# Hardware: ${s.useGPU ? "GPU (setUseGPU(True))" : "CPU (setUseGPU(False))"}`,
      },
      {
        title: "Image Extraction",
        kind: "Required",
        description: "Extract frames from Encapsulated PDF.",
        output: "Visual-NLP representation of the image, each image extracted is a new row in the dataframe.",
        mustUnderstand: [
          "Resolution controls the scale of the extracted image. Default is 300.",
          "To upscale/downscale the extracted image consider altering the Resolution.",
        ],
        controls: [
          {
            type: "range", key: "ep_resolution", label: "Resolution (DPI)",
            min: 72, max: 600, step: 72, value: 300, required: true,
            help: "DPI used to render PDF pages into images. 300 is standard; higher improves OCR on small text.",
            impact: (v) => `${v} DPI. Higher values increase accuracy and memory usage.`,
          },
        ],
        decisionSummary: (s) => {
          const compress = s.px_ocrType === "vlm";
          const res = s.px_ocrType !== "vlm" ? `, ${s.ep_resolution || 300} DPI` : "";
          return [`DicomToPdf → PdfToImage. CompressImage: ${compress}${res}.`];
        },
        code: (s) => {
          const compress = s.px_ocrType === "vlm" ? "True" : "False";
          const resolution = s.ep_resolution || 300;
          const pdfToImageBlock = compress === "True"
            ? `pdf_to_image = PdfToImage() \\
    .setInputCol("pdf") \\
    .setOutputCol("image") \\
    .setResolution(config["resolution"]) \\
    .setCompressImage(True) \\
    .setImageDimsCol("frame_dims")`
            : `pdf_to_image = PdfToImage() \\
    .setInputCol("pdf") \\
    .setOutputCol("image") \\
    .setResolution(config["resolution"]) \\
    .setCompressImage(False) \\
    .setImageDimsCol("frame_dims")`;
          return `dicom_to_pdf = DicomToPdf() \\
    .setInputCols(["content"]) \\
    .setOutputCol("pdf") \\
    .setKeepInput(False)

${pdfToImageBlock}`;
        },
      },
      sharedTextDetectionStep({ when: (s) => s.px_ocrType === "non-vlm" }),  // hidden for vlm and tesseract
      {
        title: "OCR Extraction",
        kind: "Required",
        description: "Extract visible text from rendered frames using the chosen OCR engine.",
        output: "Text and position data for each frame.",
        controls: [
          {
            type: "select", key: "px_ocrModel", label: "OCR model",
            value: "MedicalVisionLLM", required: true,
            options: (s) => s.px_ocrType === "vlm"
              ? [{ label: "MedicalVisionLLM", value: "MedicalVisionLLM", summary: "VLM OCR — highest accuracy, requires GPU and significant VRAM." }]
              : s.px_ocrType === "tesseract"
                ? [{ label: "ImageToText (Tesseract)", value: "ImageToText", summary: "Tesseract via ImageToText — CPU-native, no pretrained model download." }]
                : [
                    { label: "ImageToTextV2", value: "ImageToTextV2", summary: "Pretrained V2 OCR model — lighter alternative, works without VLM." },
                    { label: "ImageToTextV3", value: "ImageToTextV3", summary: "V3 OCR model — minimal parameter-free option." },
                  ],
            help: "Model options depend on the OCR type chosen in Pipeline Options.",
          },
          {
            type: "range", key: "ep_confidenceThreshold", label: "Confidence threshold",
            min: 0, max: 100, step: 5, value: 70, required: true,
            when: (s) => s.px_ocrType === "tesseract",
            help: "Minimum Tesseract confidence score (0–100). Characters below this are ignored.",
            impact: (v) => `Threshold ${v}. Lower values keep uncertain characters; higher values discard them.`,
          },
          {
            type: "range", key: "px_nPredict", label: "Max output tokens (nPredict)",
            min: 1024, max: 10240, step: 1024, value: 1024, required: true,
            when: (s) => s.px_ocrType === "vlm",
            help: "Caps how many tokens the VLM OCR model generates per frame. DICOM PDF frames: 3000-400 depending on density.",
            impact: (v) => `${v} tokens per frame. Increase for frames with dense text; lower saves GPU time.`,
          },
          {
            type: "range", key: "px_nCtx", label: "Context window (nCtx)",
            min: 4096, max: 65536, step: 4096, value: 32768, required: true,
            when: (s) => s.px_ocrType === "vlm",
            help: "KV cache context size. Larger values support longer outputs but use more VRAM.",
            impact: (v) => `Context window: ${v} tokens.`,
          },
          {
            type: "range", key: "px_temperature", label: "Temperature",
            min: 0, max: 1, step: 0.01, value: 0.01, required: true,
            when: (s) => s.px_ocrType === "vlm",
            help: "Sampling temperature. Keep near 0 for deterministic OCR output.",
            impact: (v) => Number(v) > 0.1 ? `Temperature ${v} — higher values may produce non-deterministic output.` : `Temperature ${v} — near-deterministic.`,
          },
          {
            type: "range", key: "px_repeatPenalty", label: "Repeat penalty",
            min: 1.0, max: 1.5, step: 0.01, value: 1.03, required: true,
            when: (s) => s.px_ocrType === "vlm",
            help: "Penalises repeated tokens. Values close to 1.0 have minimal effect.",
            impact: (v) => `Repeat penalty: ${v}.`,
          },
        ],
        decisionSummary: (s) => {
          if (s.px_ocrModel === "MedicalVisionLLM") return [`MedicalVisionLLM OCR. nPredict: ${s.px_nPredict}, nCtx: ${s.px_nCtx}, temp: ${s.px_temperature}, repeatPenalty: ${s.px_repeatPenalty}.`];
          return [`${s.px_ocrModel} OCR.`];
        },
        code: (s) => {
          if (s.px_ocrType === "vlm") {
            return `caption_assembler = DocumentAssembler() \\
    .setInputCol("caption") \\
    .setOutputCol("caption_document")

schema_converter = ImageSchemaConverter() \\
    .setInputCol("image") \\
    .setOutputCol("image_assembler") \\
    .setOutputSchema(ImageSchemaConversion.ASSEMBLER) \\
    .setKeepInput(True)

vlm_ocr = MedicalVisionLLM.pretrained("jsl-ocr-gguf-vlm1", "en", "clinical/ocr") \\
    .setInputCols(["caption_document", "image_assembler"]) \\
    .setOutputCol("completions") \\
    .setNGpuLayers(99) \\
    .setNCtx(config["n_ctx"]) \\
    .setNParallel(1) \\
    .setNBatch(2048) \\
    .setNUbatch(1024) \\
    .setNPredict(config["n_predict"]) \\
    .setTemperature(config["temperature"]) \\
    .setTopK(1) \\
    .setTopP(1.0) \\
    .setRepeatPenalty(config["repeat_penalty"]) \\
    .setRepeatLastN(256) \\
    .setStopStrings(["<｜hy_Assistant｜>", "<｜hy_place▁holder▁no▁2｜>"]) \\
    .setMinKeep(0) \\
    .setNProbs(0) \\
    .setBatchSize(1) \\
    .setDisableLog(False)

coordinate_extract = DocumentCoordinatesToText() \\
    .setInputCol("completions") \\
    .setImageDimsCol("frame_dims") \\
    .setOutputCol("text") \\
    .setPageMatrixCol("positions") \\
    .setRegionCol("regions")`;
          }
          if (s.px_ocrModel === "ImageToTextV2") {
            return `ocr = ImageToTextV2.pretrained("ocr_large_printed_v2_opt", "en", "clinical/ocr") \\
    .setRegionsColumn("text_regions") \\
    .setInputCols(["image"]) \\
    .setOutputCol("text") \\
    .setOutputFormat(OcrOutputFormat.TEXT_WITH_POSITIONS) \\
    .setGroupImages(False) \\
    .setKeepInput(True) \\
    .setUseGPU(config["use_gpu"]) \\
    .setUseCaching(True)`;
          }
          if (s.px_ocrType === "tesseract") {
            return `ocr = ImageToText() \\
    .setInputCol("image") \\
    .setOutputCol("text") \\
    .setIgnoreResolution(False) \\
    .setPageIteratorLevel(PageIteratorLevel.SYMBOL) \\
    .setPageSegMode(PageSegmentationMode.SPARSE_TEXT) \\
    .setConfidenceThreshold(config["confidence_threshold"])`;
          }
          return `ocr = ImageToTextV3() \\
    .setInputCols(["image", "text_regions"]) \\
    .setOutputCol("text")`;
        },
      },
      {
        title: "Clinical NLP Pipeline",
        kind: "Required",
        description: "Run a Healthcare NLP pretrained pipeline to detect PHI entities in the OCR text.",
        output: "NER entity chunks consumed by Position Finder.",
        mustUnderstand: [
          "The pipeline runs NER on OCR-extracted text to detect and locate PHI entities.",
          "These are state-of-the-art models — check the John Snow Labs Models Hub for the latest versions.",
          "Choose a pipeline whose entity coverage matches the PHI types present in your images.",
        ],
        controls: [
          {
            type: "select", key: "px_clinicalPipeline", label: "Pretrained pipeline",
            value: "clinical_deidentification_docwise_benchmark_medium", required: true,
            options: [
              { label: "benchmark_optimized",         value: "clinical_deidentification_docwise_benchmark_optimized",          summary: "Fastest, widest coverage. Entities: LOCATION, CONTACT, PROFESSION, NAME, DATE, AGE, MEDICALRECORD, ORGANIZATION, HEALTHPLAN, DOCTOR, USERNAME, LOCATION-OTHER, URL, DEVICE, CITY, ZIP, STATE, PATIENT, STREET, PHONE, HOSPITAL, EMAIL, IDNUM, BIOID, FAX, DLN, SSN, ACCOUNT, PLATE, VIN, LICENSE, IP." },
              { label: "benchmark_medium (default)",  value: "clinical_deidentification_docwise_benchmark_medium",             summary: "Balanced speed/accuracy. Entities: CONTACT, DATE, ID, LOCATION, PROFESSION, DOCTOR, EMAIL, PATIENT, URL, USERNAME, CITY, COUNTRY, DLN, HOSPITAL, IDNUM, LOCATION_OTHER, MEDICALRECORD, STATE, STREET, ZIP, AGE, PHONE, ORGANIZATION, SSN, ACCOUNT, PLATE, VIN, LICENSE, IP." },
              { label: "benchmark_medium_v2",         value: "clinical_deidentification_docwise_benchmark_medium_v2",          summary: "Medium v2. Entities: DATE, LOCATION, PROFESSION, DOCTOR, EMAIL, PATIENT, URL, USERNAME, CITY, COUNTRY, DLN, HOSPITAL, IDNUM, MEDICALRECORD, STATE, STREET, ZIP, AGE, PHONE, ORGANIZATION, SSN, ACCOUNT, PLATE, VIN, LICENSE, IP." },
              { label: "benchmark_large",             value: "clinical_deidentification_docwise_benchmark_large",              summary: "Larger model. Entities: CONTACT, DATE, ID, LOCATION, PROFESSION, DOCTOR, EMAIL, PATIENT, URL, USERNAME, CITY, COUNTRY, DLN, HOSPITAL, IDNUM, LOCATION_OTHER, MEDICALRECORD, STATE, STREET, ZIP, AGE, PHONE, ORGANIZATION, SSN, ACCOUNT, PLATE, VIN, LICENSE, IP." },
              { label: "benchmark_large_v2",          value: "clinical_deidentification_docwise_benchmark_large_v2",           summary: "Large v2. Entities: DATE, LOCATION, PROFESSION, DOCTOR, EMAIL, PATIENT, URL, USERNAME, CITY, COUNTRY, DLN, HOSPITAL, IDNUM, MEDICALRECORD, STATE, STREET, ZIP, AGE, PHONE, ORGANIZATION, SSN, ACCOUNT, PLATE, VIN, LICENSE, IP." },
              { label: "zeroshot_medium",             value: "clinical_deidentification_docwise_zeroshot_medium",              summary: "Zero-shot medium. Entities: DATE, PROFESSION, DOCTOR, EMAIL, PATIENT, URL, USERNAME, CITY, COUNTRY, DLN, HOSPITAL, IDNUM, MEDICALRECORD, STATE, STREET, ZIP, AGE, PHONE, ORGANIZATION, SSN, ACCOUNT, PLATE, VIN, LICENSE, IP." },
              { label: "zeroshot_large",              value: "clinical_deidentification_docwise_zeroshot_large",               summary: "Zero-shot large. Entities: DATE, PROFESSION, DOCTOR, EMAIL, PATIENT, URL, USERNAME, CITY, COUNTRY, DLN, HOSPITAL, IDNUM, MEDICALRECORD, STATE, STREET, ZIP, AGE, PHONE, ORGANIZATION, SSN, ACCOUNT, PLATE, VIN, LICENSE, IP." },
              { label: "SingleStage_zeroshot_medium", value: "clinical_deidentification_docwise_SingleStage_zeroshot_medium",  summary: "Single-stage zero-shot medium. Entities: DOCTOR, PATIENT, AGE, DATE, HOSPITAL, CITY, STREET, STATE, COUNTRY, PHONE, IDNUM, EMAIL, ZIP, ORGANIZATION, PROFESSION, USERNAME." },
              { label: "SingleStage_zeroshot_large",  value: "clinical_deidentification_docwise_SingleStage_zeroshot_large",   summary: "Single-stage zero-shot large. Entities: DOCTOR, PATIENT, AGE, DATE, HOSPITAL, CITY, STREET, STATE, COUNTRY, PHONE, IDNUM, EMAIL, ZIP, ORGANIZATION, PROFESSION, USERNAME." },
            ],
            help: "Selects the Healthcare NLP pretrained pipeline passed to PretrainedPipeline.",
          },
        ],
        decisionSummary: (s) => [`Clinical pipeline: ${s.px_clinicalPipeline}.`],
        code: (s) => `def nlp_builder(spark_session, pipeline):

    stages = list(pipeline.model.stages)
    stage_indices = {"ner_cutoff": [], "chunk_merger": [], "document_splitter": None, "sentence_detector": None}

    for idx, stage in enumerate(stages):

        stage_name = stage.uid.rsplit("_", 1)[0].lower()
        if stage_name in {"lightdeidentification", "deidentification"}:
            stage_indices["ner_cutoff"].append(idx)
        elif stage_name == "chunkmergemodel":
            stage_indices["chunk_merger"].append(idx)
        elif stage_name == "internaldocumentsplitter":
            stage_indices["document_splitter"] = idx
        elif stage_name in {"sentencedetector", "sentencedetectordlmodel"}:
            stage_indices["sentence_detector"] = idx

    if not stage_indices["ner_cutoff"]:
        raise ValueError("The pretrained pipeline does not contain a LightDeIdentification or DeIdentification stage.")

    if not stage_indices["chunk_merger"]:
        raise ValueError("The pretrained pipeline does not contain a ChunkMergeModel stage.")

    if stage_indices["document_splitter"] is None and stage_indices["sentence_detector"] is None:
        raise ValueError("The pretrained pipeline must contain InternalDocumentSplitter, SentenceDetector, or SentenceDetectorDLModel.")

    chunk_merger_idx = max(stage_indices["chunk_merger"])
    cutoff_idx = min(stage_indices["ner_cutoff"])
    ner_chunk_output_col = stages[chunk_merger_idx].getOutputCol()
    nlp_pipeline = Pipeline(stages=stages[:cutoff_idx])
    empty_data = spark_session.createDataFrame([[""]], ["text"])
    nlp_model = nlp_pipeline.fit(empty_data)

    return ner_chunk_output_col, nlp_model

pretrained_pipeline = PretrainedPipeline(config["clinical_pipeline"], "en", "clinical/models")
ner_output, nlp_pipeline = nlp_builder(spark, pretrained_pipeline)`,
      },
      validateSaveStage(() => "dicom"),
    ],
  fullCode(s) {
  const allSteps = this.steps;
  const vlm = s.px_ocrType === "vlm";
  const tesseract = s.px_ocrType === "tesseract";
  const ocrModelName = vlm ? "MedicalVisionLLM" : tesseract ? "ImageToText" : (s.px_ocrModel && s.px_ocrModel !== "MedicalVisionLLM" ? s.px_ocrModel : "ImageToTextV2");

  const stages = vlm
    ? ["dicom_to_pdf", "pdf_to_image", "caption_assembler", "schema_converter", "vlm_ocr", "coordinate_extract", "schema_converter_internal", "nlp_pipeline", "position_finder", "image_draw_regions", "image_to_pdf", "dicom_update_pdf"]
    : tesseract
      ? ["dicom_to_pdf", "pdf_to_image", "ocr", "nlp_pipeline", "position_finder", "image_draw_regions", "image_to_pdf", "dicom_update_pdf"]
      : ["dicom_to_pdf", "pdf_to_image", "ocr", "nlp_pipeline", "position_finder", "image_draw_regions", "image_to_pdf", "dicom_update_pdf"];
  const ocrImport = vlm
    ? "MedicalVisionLLM, ImageSchemaConverter, DocumentCoordinatesToText"
    : tesseract
      ? "ImageToText"
      : ocrModelName;

  const stepByTitle = (title) => allSteps.find((st) => st.title === title);
  const imageStep    = stepByTitle("Image Extraction");
  const textDetStep  = stepByTitle("Text Detection");
  const ocrStep      = stepByTitle("OCR Extraction");
  const nlpStep      = stepByTitle("Clinical NLP Pipeline");

  const saveStep     = stepByTitle("Validate and Save");

  const includeTextDet = !vlm && !tesseract && textDetStep && isStepVisible(allSteps.indexOf(textDetStep)) && isStageConfirmed(allSteps.indexOf(textDetStep));
  const textDetCode    = includeTextDet ? textDetStep.code(s) + "\n\n" : "";
  if (includeTextDet) stages.splice(2, 0, "text_detector");


  const textDetImport = (!tesseract && includeTextDet) ? `, ${s.textDetector || "ImageTextDetector"}` : "";

  return `import os
import json
import sys
import shutil

import sparknlp
import sparknlp_jsl

from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp_jsl.annotator import *

import sparkocr
from sparkocr.transformers import *
from sparkocr.enums import *
from sparkocr.utils import *
from sparkocr.schemas import *

from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import functions as F

config = {
    "use_gpu": ${pyBool(s.useGPU)},
    "ocr_model": "${ocrModelName}",
    "resolution": ${s.ep_resolution || 300},\n    ${tesseract ? '"confidence_threshold": ' + (s.ep_confidenceThreshold || 70) + ',\n    ' : ""}
    ${vlm ? vlmConfigLines(s) : textDetectorConfigLines(s)}
    "clinical_pipeline": "${s.px_clinicalPipeline}",
    "final_dicom_col": "dicom",
}

${imageStep ? imageStep.code(s) : ""}

${textDetCode}${ocrStep ? ocrStep.code(s) : ""}

${vlm ? `schema_converter_internal = ImageSchemaConverter() \\
    .setInputCol("image_assembler") \\
    .setOutputCol("image") \\
    .setOutputSchema(ImageSchemaConversion.INTERNAL) \\
    .setKeepInput(False)

` : ""}${nlpStep ? nlpStep.code(s) : ""}

position_finder = PositionFinder() \\
    .setInputCols([ner_output]) \\
    .setOutputCol("ner_coordinates") \\
    .setPageMatrixCol("positions")

image_draw_regions = ImageDrawRegions() \\
    .setInputCol("image") \\
    .setInputRegionsCol("ner_coordinates") \\
    .setOutputCol("image_with_regions") \\
    .setFilledRect(True) \\
    .setRectColor(Color.black)

image_to_pdf = ImageToPdf() \\
    .setInputCol("image_with_regions") \\
    .setOutputCol("pdf")

dicom_update_pdf = DicomUpdatePdf() \\
    .setInputCol("path") \\
    .setInputPdfCol("pdf") \\
    .setOutputCol("dicom") \\
    .setKeepInput(True)

pipeline = PipelineModel(stages=[
    ${stages.join(",\n    ")},
])

${vlm ? `vision_prompt = "Detect and recognize text in the image, and output the text coordinates in a formatted manner."

dicom_df = spark.read.format("binaryFile").load("${FIXED.dicomPath}").withColumn("caption", F.lit(vision_prompt))` : `dicom_df = spark.read.format("binaryFile").load("${FIXED.dicomPath}")`}
result = pipeline.transform(dicom_df).cache()

${saveStep ? saveStep.code(s) : ""}`;
},
};