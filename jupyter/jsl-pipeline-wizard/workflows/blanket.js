const workflowBlanket = {
    name: "DICOM Blanket Pixel De-id",
    description: "Redact every detected text region on DICOM pixels — no OCR or NER required.",
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
          "Blanket-redacts every visible text region on DICOM pixel data.",
          "Uses only a text detector — no OCR or NER required.",
          ],
          outputs: [
          "DICOM files with all detected text regions blacked out.",
          ],
          prerequisites: [
          "DICOM files with burned-in text on pixel data.",
          ],
          whenToUse: [
          "When speed and cost matter more than surgical PHI targeting.",
          "When all visible text on the pixel must be removed regardless of content.",
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
          "Blanket mode removes all detected text regions from the DICOM slices.",
          "Only Text Detection model is used in this workflow.",
        ],
        controls: [
          {
            type: "choice", key: "useGPU", label: "Hardware",
            required: true,
            options: [
              {
                label: "CPU",
                value: false,
                summary: "Run on CPU. Compatible with all clusters. Slower for large datasets.",
              },
              {
                label: "GPU",
                value: true,
                summary: "Run on GPU. Requires a CUDA-capable GPU in the cluster. Fastest option.",
              },
            ],
            help: "Select GPU only if your cluster has a CUDA-capable GPU attached.",
            impact: (v) => v ? "Models will use GPU acceleration (setUseGPU(True))." : "Models will run on CPU (setUseGPU(False)).",
          }
        ],
        decisionSummary: (s) => [
          `Hardware: ${s.useGPU ? "GPU" : "CPU"}.`,
        ],
        code: (s) => `# Hardware: ${s.useGPU ? "GPU (setUseGPU(True))" : "CPU (setUseGPU(False))"}`,
      },
      sharedImageExtractionStep({ compressMode: "blanket" }),
      sharedTextDetectionStep({ includeDrawRegions: true }),
      validateSaveStage(() => FIXED.pixelOutputCol),
    ],
  fullCode(s) {
  const allSteps = this.steps;
  const visibleSteps = allSteps.filter((st, i) => isStepVisible(i));

  const stages = ["dicom_to_image", "text_detector", "draw_regions"];

  // Find steps by title for safe indexing
  const stepByTitle = (title) => allSteps.find((st) => st.title === title);
  const imageStep    = stepByTitle("Image Extraction");
  const detectStep   = stepByTitle("Text Detection");
  const saveStep     = stepByTitle("Validate and Save");


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
    "text_detector": "${s.textDetector}",
    "score_threshold": ${s.scoreThreshold},
    "text_threshold": ${s.textThreshold || 0.5},
    "size_threshold": ${s.sizeThreshold || 10},
    ${s.textDetector === "ImageTextDetector" ? '"link_threshold": ' + (s.linkThreshold || 0.5) + ',' : ""}
    "with_refiner": ${pyBool(s.withRefiner)},
    "scale": ${s.scale},
    "frame_limit": ${s.enableFrameSampling !== false ? s.frameLimit : 0},
    ${s.enableFrameSampling !== false ? '"frame_sampling_strategy": FrameSamplingStrategy.' + s.frameStrategy + ',' : ""}
    "compression_mode": "${s.compressionMode || "disabled"}",
    ${(s.compressionMode || "disabled") !== "disabled" ? '"compression_quality": ' + (s.compressionQuality || 85) + ',\n    ' : ""}${(s.compressionMode || "disabled") === "auto" ? '"compression_threshold": ' + (s.compressionThreshold || 1) + ',\n    ' : ""}"final_dicom_col": "${FIXED.pixelOutputCol}",
}

${imageStep ? imageStep.code(s) : ""}

${detectStep ? detectStep.code(s) : ""}

pipeline = PipelineModel(stages=[${stages.join(", ")}])

dicom_df = spark.read.format("binaryFile").load("${FIXED.dicomPath}")
result = pipeline.transform(dicom_df).cache()

${saveStep ? saveStep.code(s) : ""}`;
},
};