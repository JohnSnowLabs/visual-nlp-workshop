// Shared step factory functions — used by blanket.js and pixel-phi.js.
// Changes here apply to both workflows automatically.

// ── Shared config-dict line builders ─────────────────────────────────────────
// Call these inside fullCode() template literals to build the config dict.
// Any change to key names or logic propagates to all workflows automatically.

function textDetectorConfigLines(s) {
  const link = (s.textDetector || "ImageTextDetector") === "ImageTextDetector"
    ? `\n    "link_threshold": ${s.linkThreshold || 0.5},`
    : "";
  return `"text_detector": "${s.textDetector || "ImageTextDetector"}",
    "score_threshold": ${s.scoreThreshold},
    "text_threshold": ${s.textThreshold || 0.5},
    "size_threshold": ${s.sizeThreshold || 10},${link}
    "with_refiner": ${pyBool(s.withRefiner)},`;
}

function vlmConfigLines(s) {
  return `"n_predict": ${s.px_nPredict},
    "n_ctx": ${s.px_nCtx},
    "temperature": ${s.px_temperature},
    "repeat_penalty": ${s.px_repeatPenalty},`;
}

function compressionConfigLines(s, defaultMode = "enabled") {
  const mode = s.compressionMode || defaultMode;
  const quality = mode !== "disabled" ? `\n    "compression_quality": ${s.compressionQuality || 85},` : "";
  const thresh  = mode === "auto"     ? `\n    "compression_threshold": ${s.compressionThreshold || 1},` : "";
  return `"compression_mode": "${mode}",${quality}${thresh}`;
}


function sharedImageExtractionStep(opts = {}) {
  const isBlanket = opts.compressMode === "blanket";

  const compressionControls = isBlanket
    ? [
        {
          type: "select", key: "compressionMode", label: "Compression mode",
          value: "disabled", required: true,
          options: [
            { label: "disabled", value: "disabled", summary: "No compression — recommended for blanket redaction." },
            { label: "auto",     value: "auto",     summary: "Compress only files above the size threshold." },
            { label: "enabled",  value: "enabled",  summary: "Always compress output images." },
          ],
          help: "JPEG-compress rendered frames. Use Auto to let the pipeline decide based on frame size."
        },
        {
          type: "range", key: "compressionQuality", label: "Compression quality",
          min: 1, max: 95, step: 1, value: 85, required: false,
          when: (s) => s.compressionMode !== "disabled",
          help: "JPEG quality (1–95). Higher = better quality, larger file size.",
          impact: (v) => `Quality ${v}. Values below 70 may degrade text legibility.`,
        },
        {
          type: "range", key: "compressionThreshold", label: "Compression threshold (MB)",
          min: 1, max: 50, step: 1, value: 1, required: false,
          when: (s) => s.compressionMode === "auto",
          help: "Files larger than this threshold (MB) will be compressed in auto mode.",
          impact: (v) => `Compress frames larger than ${v} MB.`,
        },
      ]
    : [
        {
          type: "select", key: "compressionMode", label: "Compression mode",
          value: "enabled", required: true,
          options: [
            { label: "disabled", value: "disabled", summary: "No compression — use for non-VLM OCR." },
            { label: "enabled",  value: "enabled",  summary: "Always compress — required for VLM OCR." },
            { label: "auto",     value: "auto",     summary: "Compress only frames above the size threshold." },
          ],
          help: "VLM requires enabled or auto. Non-VLM should use disabled.",
          impact: (v) => v === "disabled" ? "No compression applied." : v === "auto" ? "Compress frames above size threshold." : "All frames compressed.",
        },
        {
          type: "range", key: "compressionQuality", label: "Compression quality",
          min: 1, max: 95, step: 1, value: 85, required: false,
          when: (s) => s.compressionMode !== "disabled",
          help: "JPEG quality (1–95) for compressed frames passed to VLM.",
          impact: (v) => `Quality ${v}. Values below 70 may reduce VLM text recognition accuracy.`,
        },
        {
          type: "range", key: "compressionThreshold", label: "Compression threshold (MB)",
          min: 1, max: 50, step: 1, value: 1, required: false,
          when: (s) => s.compressionMode === "auto",
          help: "Files larger than this threshold (MB) will be compressed in auto mode.",
          impact: (v) => `Compress frames larger than ${v} MB.`,
        },
      ];

  return {
    title: "Image Extraction",
    kind: "Required",
    description: isBlanket
      ? "Extract Images from DICOM slices."
      : "Extract Images from DICOM slices.",
    output: isBlanket
      ? "Visual-NLP representation of the image, each image extracted is a new row in the dataframe."
      : "Visual-NLP representation of the image, each image extracted is a new row in the dataframe.",
    mustUnderstand: isBlanket
      ? [
          "Frame sampling limits processing to N evenly-spaced frames. When disabled, all frames are processed.",
          "Sampling Strategy decides how the frames will be selected.",
          "Scale factor applied to images before processing. 1.0 = original size, < 1.0 = shrink, > 1.0 = enlarge.",
          "Images can be compressed to reduce memory usage and speed up inference.",
        ]
      : [
          "Frame sampling limits processing to N evenly-spaced frames. When disabled, all frames are processed.",
          "Sampling Strategy decides how the frames will be selected.",
          "Scale factor applied to images before processing. 1.0 = original size, < 1.0 = shrink, > 1.0 = enlarge.",
          "Images can be compressed to reduce memory usage and speed up inference.",
        ],
    controls: [
      {
        type: "toggle", key: "enableFrameSampling", label: "Enable frame sampling",
        value: true, required: true,
        help: "When off, all frames are processed (frame_limit = 0). When on, only the configured number of frames are sampled.",
        impact: (v) => v ? "Frame sampling on — only selected frames processed." : "Frame sampling off — all frames processed (frame_limit = 0).",
      },
      {
        type: "range", key: "frameLimit", label: "Frame limit",
        min: 1, max: 50, value: 5, required: true,
        when: (s) => s.enableFrameSampling !== false,
        help: "Maximum frames processed per DICOM file.",
        impact: (v) => `${v} frame${Number(v) === 1 ? "" : "s"} processed per file.`,
      },
      {
        type: "select", key: "frameStrategy", label: "Sampling strategy",
        value: "CONSECUTIVE", required: true,
        when: (s) => s.enableFrameSampling !== false,
        options: [
          { label: "CONSECUTIVE", value: "CONSECUTIVE", summary: "First N frames. Predictable and reproducible." },
          { label: "MIDDLE",      value: "MIDDLE",      summary: "Middle frames. Good when labels appear in representative central slices." },
          { label: "STRIDE",      value: "STRIDE",      summary: "Spread across the study. Better for long multi-frame inputs." },
          { label: "RANDOM",      value: "RANDOM",      summary: "Random sample. Useful for exploration, less reproducible." },
        ],
        help: "Which frames are chosen when frame limit < total frame count.",
      },
      {
        type: "range", key: "scale", label: "Image scale",
        min: 0.5, max: 2, step: 0.25, value: 1, required: true,
        help: "Scale applied during image extraction.",
        impact: (v) => `${v}x. Scale correction 1/${v} applied during redaction.`,
      },
      ...compressionControls,
    ],
    decisionSummary: (s) => {
      const sampling = s.enableFrameSampling !== false
        ? `${s.frameLimit} frame(s), ${s.frameStrategy} sampling`
        : "all frames (sampling off)";
      if (isBlanket) {
        const mode = s.compressionMode || "disabled";
        return [
          `${sampling}, ${s.scale}x scale.`,
          `Compression: ${mode}${mode !== "disabled" ? `, quality ${s.compressionQuality}` : ""}.`,
        ];
      }
      const mode = s.compressionMode || "enabled";
      const compInfo = mode !== "disabled"
        ? `, compression ${mode} (quality ${s.compressionQuality})`
        : ", compression disabled";
      return [`${sampling}, ${s.scale}x scale${compInfo}.`];
    },
    code: (s) => {
      let compress, qualityLine, threshLine;
      if (isBlanket) {
        const mode = s.compressionMode || "disabled";
        compress    = "False";
        qualityLine = mode !== "disabled" ? ` \\\n    .setCompressionQuality(config["compression_quality"])` : "";
        threshLine  = mode === "auto"     ? ` \\\n    .setCompressionThreshold(config["compression_threshold"])` : "";
      } else {
        const mode = s.compressionMode || "enabled";
        compress    = mode !== "disabled" ? "True" : "False";
        qualityLine = mode !== "disabled" ? ` \\
    .setCompressionQuality(config["compression_quality"])` : "";
        threshLine  = mode === "auto"     ? ` \\
    .setCompressionThreshold(config["compression_threshold"])` : "";
      }
      const strategyLine = s.enableFrameSampling !== false ? ` \\
    .setFrameSamplingStrategy(config["frame_sampling_strategy"])` : "";
      return `dicom_to_image = DicomToImageV3() \\
    .setInputCols(["content"]) \\
    .setOutputCol("${FIXED.imageCol}") \\
    .setKeepInput(False) \\
    .setScale(config["scale"]) \\
    .setFrameLimit(config["frame_limit"])${strategyLine} \\
    .setCompressImage(${compress}) \\
    .setCompressionMode(config["compression_mode"])${qualityLine}${threshLine} \\
    .setFrameDimsCol("frame_dims")`;
    },
  };
}

function sharedTextDetectionStep(opts = {}) {
  return {
    title: "Text Detection",
    ...(opts.when ? { when: opts.when } : {}),
    kind: "Required",
    description: "Detect visible text regions on the rendered DICOM image.",
    output: "Visual NLP representation of bounding boxes.",
    mustUnderstand: [
      "ImageTextDetector is Scala based implementation using ONNX as the inference engine.",
      "ImageTextDetectorV2 is Python based implementation using Pytorch as the inference engine.",
      "Score threshold filters the regions based on the models confidence score.",

    ],
    controls: [
      {
        type: "select", key: "textDetector", label: "Detector",
        value: "ImageTextDetector", required: true,
        options: [
          { label: "ImageTextDetector",   value: "ImageTextDetector",   summary: "Default pretrained detector (memory-optimised)." },
          { label: "ImageTextDetectorV2", value: "ImageTextDetectorV2", summary: "Alternative detector implementation." },
        ],
        help: "Both detect regions. Neither performs OCR recognition.",
      },
      {
        type: "range", key: "scoreThreshold", label: "Score threshold",
        min: 0.1, max: 0.95, step: 0.05, value: 0.5, required: true,
        help: "Detection confidence threshold.",
        impact: (v) => Number(v) < 0.45 ? "Permissive — may redact more non-text areas." : "Stricter — may miss faint text.",
      },
      {
        type: "range", key: "textThreshold", label: "Text threshold",
        min: 0.1, max: 0.95, step: 0.05, value: 0.5, required: true,
        help: "Threshold for classifying a detected region as text vs background.",
        impact: (v) => `Text threshold: ${v}.`,
      },
      {
        type: "range", key: "sizeThreshold", label: "Size threshold",
        min: 1, max: 100, step: 1, value: 10, required: true,
        help: "Minimum pixel size of detected regions to keep. Smaller values catch tiny text but may increase noise.",
        impact: (v) => `Size threshold: ${v}px.`,
      },
      {
        type: "range", key: "linkThreshold", label: "Link threshold",
        min: 0.1, max: 0.95, step: 0.05, value: 0.5, required: true,
        when: (s) => s.textDetector === "ImageTextDetector",
        help: "Threshold for linking adjacent character regions. Only applies to ImageTextDetector.",
        impact: (v) => `Link threshold: ${v}.`,
      },
      {
        type: "toggle", key: "withRefiner", label: "Use detector refiner",
        value: true, required: true,
        help: "Post-processing pass that can refine detected text boxes.",
        impact: (v) => v ? "Refiner enabled — may improve box quality." : "No refiner pass.",
      },
    ],
    decisionSummary: (s) => [
      `Detector: ${s.textDetector}.`,
      `Score ${s.scoreThreshold}, text ${s.textThreshold}, size ${s.sizeThreshold}${s.textDetector === "ImageTextDetector" ? ", link " + s.linkThreshold : ""}.`,
      `Refiner ${s.withRefiner ? "on" : "off"}.`,
    ],
    code: (s) => {
      const model = s.textDetector === "ImageTextDetectorV2" ? "image_text_detector_v2" : "image_text_detector_mem_opt";
      const link  = s.textDetector === "ImageTextDetector" ? ` \\\n    .setLinkThreshold(config["link_threshold"])` : "";
      const detector = `text_detector = ${s.textDetector}.pretrained("${model}", "en", "clinical/ocr") \\
    .setInputCol("${FIXED.imageCol}") \\
    .setOutputCol("${FIXED.textRegionsCol}") \\
    .setScoreThreshold(config["score_threshold"])${link} \\
    .setTextThreshold(config["text_threshold"]) \\
    .setSizeThreshold(config["size_threshold"]) \\
    .setWithRefiner(config["with_refiner"]) \\
    .setUseGPU(config["use_gpu"])`;
      if (!opts.includeDrawRegions) return detector;
      return detector + `\n\ndraw_regions = DicomDrawRegions() \\
    .setInputCol("path") \\
    .setInputRegionsCol("${FIXED.textRegionsCol}") \\
    .setOutputCol("${FIXED.pixelOutputCol}") \\
    .setAggCols(["path"]) \\
    .setKeepInput(True) \\
    .setScaleFactor(1 / config["scale"])`;
    },
  };
}

function sharedExtractMetadataStep(opts = {}) {
  const nerKey  = opts.nerKey  || "extractTagForNER";
  const inputCol = opts.inputCol || FIXED.pixelOutputCol;
  return {
    title: "Extract Metadata",
    when: opts.when,
    kind: "Optional",
    description: "Extract DICOM metadata tags into a structured column after pixel redaction.",
    output: "`metadata_original` map column. Pixel-redacted bytes are preserved via `setKeepInput(True)`.",
    mustUnderstand: [
      "Enable `setExtractTagForNER` only when running NER on free-text tag values — a strategy file is then required.",
    ],
    controls: [
      {
        type: "toggle", key: nerKey, label: "Extract tags for NER (setExtractTagForNER)",
        value: false, required: true,
        help: "Flattens free-text tag values for NER input. Requires a strategy file.",
        impact: (v) => v ? "Free-text tags extracted for NER — strategy file required." : "Standard tag extraction only.",
      },
    ],
    decisionSummary: (s) => {
      const lines = ["Metadata extraction included."];
      if (s[nerKey]) lines.push("NER extraction enabled.");
      return lines;
    },
    code: (s) => {
      if (!s[nerKey]) return "";
      return `strategy_file_path = "${FIXED.strategyFilePath}"

metadata_extractor = DicomToMetadata() \\
    .setInputCol("${inputCol}") \\
    .setOutputCol("metadata_original") \\
    .setKeepInput(True) \\
    .setExtractTagForNer(True) \\
    .setStrategyFile(strategy_file_path)`;
    },
  };
}
