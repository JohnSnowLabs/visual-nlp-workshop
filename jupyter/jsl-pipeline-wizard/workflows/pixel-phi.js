const workflowPixelPhi = {
    name: "DICOM PHI-Only Pixel De-id",
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
          "Detects and redacts only PHI regions on DICOM pixel data using OCR + NLP.",
          "Supports VLM OCR (GPU) or non-VLM OCR (CPU/GPU) with a Healthcare NLP clinical pipeline.",
          ],
          outputs: [
          "DICOM files with only PHI text regions redacted, non-PHI text preserved.",
          ],
          prerequisites: [
          "DICOM files with burned-in PHI on pixel data.",
          "A GPU cluster if using VLM OCR; CPU or GPU for non-VLM.",
          ],
          whenToUse: [
          "When you need precise PHI-only redaction while preserving non-PHI annotations.",
          "When regulatory compliance requires targeted de-id over blanket removal.",
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
      "VLM OCR (MedicalVisionLLM) — GPU only",
      "Non-VLM OCR (ImageToTextV2/V3) - V3 is CPU-only, V2 supports GPU.",
      "Text detectors (ImageTextDetector, ImageTextDetectorV2) — support both CPU and GPU.",
        ],
        controls: [
          {
            type: "choice", key: "px_ocrType", label: "OCR type",
            required: true,
            options: [
              { label: "VLM OCR",     value: "vlm",     summary: "MedicalVisionLLM — highest accuracy. Requires GPU." },
              { label: "Non-VLM OCR", value: "non-vlm", summary: "ImageToTextV2 or V3 — lighter, CPU-compatible." },
            ],
            help: "VLM requires a GPU. Non-VLM supports both GPU and CPU.",
            impact: (v) => v === "vlm" ? "VLM selected — GPU required, Text Detection hidden." : "Non-VLM selected — Text Detection available, CPU supported.",
            onSelect: (v, settings) => {
              if (v === "vlm") settings.useGPU = true;
              if (v === "non-vlm") settings.px_ocrModel = "ImageToTextV2";
            },
          },
          {
            type: "choice", key: "useGPU", label: "Hardware",
            required: true,
            options: (s) => {
              const gpu = { label: "GPU", value: true,  summary: "Run on GPU. Required for VLM OCR. Fastest option." };
              const cpu = { label: "CPU", value: false, summary: "Run on CPU. Only valid with Non-VLM OCR." };
              return s.px_ocrType === "vlm" ? [gpu] : [gpu, cpu];
            },
            help: "VLM OCR requires GPU. Non-VLM OCR also supports CPU.",
            impact: (v) => v ? "Models will use GPU acceleration (setUseGPU(True))." : "Models will run on CPU (setUseGPU(False)).",
          }
        ],
        decisionSummary: (s) => [
          `OCR type: ${s.px_ocrType === "vlm" ? "VLM OCR" : "Non-VLM OCR"}.`,
          `Hardware: ${s.useGPU ? "GPU" : "CPU"}.`,
        ],
        code: (s) => `# Hardware: ${s.useGPU ? "GPU (setUseGPU(True))" : "CPU (setUseGPU(False))"}`,
      },
      sharedImageExtractionStep({ compressMode: "vlm_conditional" }),
      sharedTextDetectionStep({ when: (s) => s.px_ocrType === "non-vlm" }),
      {
        title: "OCR Extraction",
        kind: "Required",
        description: "Extract visible text from rendered frames using the chosen OCR engine.",
        output: "Visual NLP representation of text and character level positions.",
        mustUnderstand: (s) => s.px_ocrType === "vlm"
          ? [
              "MedicalVisionLLM is a 1B OCR model and does not require an external text detector.",
              "VLM requires a GPU and significant VRAM.",
            ]
          : [
              "ImageToTextV2/V3 runs a two-stage pipeline: text detection first, then OCR.",
              "ImageToTextV3 is CPU-only. ImageToTextV2 supports both GPU and CPU.",
            ],
        controls: [
          {
            type: "select", key: "px_ocrModel", label: "OCR model",
            value: "MedicalVisionLLM", required: true,
            options: (s) => s.px_ocrType === "vlm"
              ? [{ label: "MedicalVisionLLM", value: "MedicalVisionLLM", summary: "VLM OCR — highest accuracy, requires GPU and significant VRAM." }]
              : [
                  { label: "ImageToTextV2", value: "ImageToTextV2", summary: "Pretrained V2 OCR model — lighter alternative, works without VLM." },
                  { label: "ImageToTextV3", value: "ImageToTextV3", summary: "V3 OCR model — minimal parameter-free option." },
                ],
            help: "Model options depend on the OCR type chosen in Pipeline Options.",
          },
          {
            type: "range", key: "px_nPredict", label: "Max output tokens (nPredict)",
            min: 512, max: 2048, step: 128, value: 1024, required: true,
            when: (s) => s.px_ocrType === "vlm",
            help: "Caps how many tokens the VLM OCR model generates per frame. DICOM frames: 1000–2000.",
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
          return `ocr = ImageToTextV3() \\
    .setInputCols(["image", "text_regions"]) \\
    .setOutputCol("text")`;
        },
      },
      {
        title: "Clinical NLP Pipeline",
        kind: "Required",
        description: "Run a Healthcare NLP pretrained pipeline to detect PHI entities on the extracted text.",
        output: "Healthcare NLP internal representation of NER outputs.",
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
      validateSaveStage(() => FIXED.pixelOutputCol),
    ],
  fullCode(s) {
  const allSteps = this.steps;
  const vlm = s.px_ocrType === "vlm";
  const ocrModelName = vlm ? "MedicalVisionLLM" : (s.px_ocrModel && s.px_ocrModel !== "MedicalVisionLLM" ? s.px_ocrModel : "ImageToTextV2");

  const stages = vlm
    ? ["dicom_to_image", "caption_assembler", "schema_converter", "vlm_ocr", "coordinate_extract", "nlp_pipeline", "position_finder", "draw_regions"]
    : ["dicom_to_image", "ocr", "nlp_pipeline", "position_finder", "draw_regions"];
  const ocrImport = vlm
    ? "MedicalVisionLLM, ImageSchemaConverter, DocumentCoordinatesToText, DocumentAssembler"
    : ocrModelName;

  const stepByTitle = (title) => allSteps.find((st) => st.title === title);
  const imageStep    = stepByTitle("Image Extraction");
  const textDetStep  = stepByTitle("Text Detection");
  const ocrStep      = stepByTitle("OCR Extraction");
  const nlpStep      = stepByTitle("Clinical NLP Pipeline");

  const saveStep     = stepByTitle("Validate and Save");

  const includeTextDet = !vlm && textDetStep && isStepVisible(allSteps.indexOf(textDetStep)) && isStageConfirmed(allSteps.indexOf(textDetStep));
  const textDetCode    = includeTextDet ? textDetStep.code(s) + "\n\n" : "";
  if (includeTextDet) stages.splice(1, 0, "text_detector");


  const textDetImport = includeTextDet ? `, ${s.textDetector || "ImageTextDetector"}` : "";

  return `import os
import json 
import sys
import shutil 

from sparknlp.annotator import *
from sparknlp.base import *
import sparknlp_jsl
from sparknlp_jsl.annotator import *

import sparkocr
from sparkocr.transformers import *
from sparkocr.utils import *
from sparkocr.enums import *
from sparkocr.schemas import *

from pyspark.ml import PipelineModel, Pipeline
import pyspark.sql.functions as F

config = {
    "use_gpu": ${pyBool(s.useGPU)},
    "ocr_model": "${ocrModelName}",
    "scale": ${s.scale},
    "frame_limit": ${s.enableFrameSampling !== false ? s.frameLimit : 0},
    ${s.enableFrameSampling !== false ? '"frame_sampling_strategy": FrameSamplingStrategy.' + s.frameStrategy + ',' : ""}
    ${compressionConfigLines(s, "enabled")}
    ${vlm ? vlmConfigLines(s) : textDetectorConfigLines(s)}
    "clinical_pipeline": "${s.px_clinicalPipeline}",
    "final_dicom_col": "${FIXED.pixelOutputCol}",
}

${imageStep ? imageStep.code(s) : ""}

${textDetCode}${ocrStep ? ocrStep.code(s) : ""}

${nlpStep ? nlpStep.code(s) : ""}

position_finder = PositionFinder() \\
    .setInputCols([ner_output]) \\
    .setOutputCol("ner_coordinates") \\
    .setPageMatrixCol("positions")

draw_regions = DicomDrawRegions() \\
    .setInputCol("path") \\
    .setInputRegionsCol("ner_coordinates") \\
    .setOutputCol("${FIXED.pixelOutputCol}") \\
    .setAggCols(["path"]) \\
    .setKeepInput(True) \\
    .setScaleFactor(1 / config["scale"])

pipeline = PipelineModel(stages=[${stages.join(", ")}])

${vlm ? `vision_prompt = "Detect and recognize text in the image, and output the text coordinates in a formatted manner."

dicom_df = spark.read.format("binaryFile").load("${FIXED.dicomPath}").withColumn("caption", F.lit(vision_prompt))` : `dicom_df = spark.read.format("binaryFile").load("${FIXED.dicomPath}")`}
result = pipeline.transform(dicom_df).cache()

${saveStep ? saveStep.code(s) : ""}`;
},
};