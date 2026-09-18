/**
 * test-codegen.js
 * Run with:  node test-codegen.js
 *
 * For each workflow, generates fullCode() output for:
 *   • the default settings
 *   • one variant per key config option (one change at a time)
 * Writes every output to test-output/<workflow>/<variant>.py
 */

const fs   = require("fs");
const path = require("path");
const vm   = require("vm");

// ── Load all workflow files into a shared sandbox ───────────────────────────
const sandbox = {
  // Browser stubs
  console,
  document: { createElement: () => ({}) },

  // App stubs needed by utils.js / workflow files
  state: { settings: {}, strategyFiles: {} },
  isStepVisible: () => true,
  isStageConfirmed: () => true,
};

const FILES = [
  "utils.js",
  "workflows/common.js",
  "workflows/blanket.js",
  "workflows/pixel-phi.js",
  "workflows/encapsulated-pdf.js",
  "workflows/metadata.js",
  "workflows/strategy-file.js",
];

const ctx = vm.createContext(sandbox);
for (const rel of FILES) {
  const raw = fs.readFileSync(path.join(__dirname, rel), "utf8");
  // Rewrite top-level const/let → var so declarations land on the sandbox
  const src = raw.replace(/^const /gm, 'var ').replace(/^let /gm, 'var ');
  vm.runInContext(src, ctx, { filename: rel });
}

const {
  workflowBlanket, workflowPixelPhi, workflowEncapsulatedPdf,
  workflowMetadata, workflowStrategyFile,
} = sandbox;

// ── Default settings per workflow ────────────────────────────────────────────
const BLANKET_DEFAULTS = {
  useGPU: false,
  // Image Extraction
  enableFrameSampling: true, frameLimit: 5, frameStrategy: "CONSECUTIVE", scale: 1,
  compressionMode: "disabled", compressionQuality: 85, compressionThreshold: 1,
  // Text Detection
  textDetector: "ImageTextDetector", scoreThreshold: 0.5, textThreshold: 0.5,
  sizeThreshold: 10, linkThreshold: 0.5, withRefiner: true,
  // Save
  includeSaveHelper: "yes",
};

const PIXEL_PHI_DEFAULTS = {
  useGPU: true, px_ocrType: "vlm", px_ocrModel: "MedicalVisionLLM",
  enableFrameSampling: true, frameLimit: 5, frameStrategy: "CONSECUTIVE", scale: 1,
  compressionMode: "enabled", compressionQuality: 85, compressionThreshold: 1,
  px_nPredict: 256, px_nCtx: 2048, px_temperature: 0.01, px_repeatPenalty: 1.0,
  px_clinicalPipeline: "ner_deid_large_clinical",
  // non-vlm fields (used when ocrType switches)
  textDetector: "ImageTextDetector", scoreThreshold: 0.5, textThreshold: 0.5,
  sizeThreshold: 10, linkThreshold: 0.5, withRefiner: true,
  includeSaveHelper: "yes",
};

const ENC_PDF_DEFAULTS = {
  useGPU: true, px_ocrType: "vlm", px_ocrModel: "MedicalVisionLLM",
  ep_resolution: 300,
  px_nPredict: 256, px_nCtx: 2048, px_temperature: 0.01, px_repeatPenalty: 1.0,
  px_clinicalPipeline: "ner_deid_large_clinical",
  textDetector: "ImageTextDetector", scoreThreshold: 0.5, textThreshold: 0.5,
  sizeThreshold: 10, linkThreshold: 0.5, withRefiner: true,
  includeSaveHelper: "yes",
};

const METADATA_DEFAULTS = {
  useGPU: false,
  md_extractTagForNer: false,
  md_nerModels: ["zeroshot_ner_deid_subentity_merged_medium"],
  md_removePrivateTags: false,
  md_includeGroupStrategy: false,
  includeSaveHelper: "yes",
};

const STRATEGY_DEFAULTS = {
  sf_name: "my_strategy",
  sf_delivery: "render",
  sf_includeGroup: false,
  sf_tagRows: [
    { tag: "(0010, 0010)", vr: "PN", name: "Patient Name",  action: "replaceWithLiteral", option: "<REMOVED>" },
    { tag: "(0010, 0020)", vr: "LO", name: "Patient ID",    action: "hashId",             option: "" },
    { tag: "(0010, 0030)", vr: "DA", name: "Patient DOB",   action: "remove",             option: "" },
  ],
  sf_groupRows: [],
};

// ── Variants (one setting change per variant) ─────────────────────────────────
const VARIANTS = {
  blanket: [
    { label: "default" },
    { label: "gpu",                   patch: { useGPU: true } },
    { label: "frame-sampling-off",    patch: { enableFrameSampling: false } },
    { label: "frame-strategy-stride", patch: { frameStrategy: "STRIDE", frameLimit: 10 } },
    { label: "scale-2x",              patch: { scale: 2 } },
    { label: "compression-auto",      patch: { compressionMode: "auto", compressionQuality: 75, compressionThreshold: 5 } },
    { label: "compression-enabled",   patch: { compressionMode: "enabled", compressionQuality: 70 } },
    { label: "detector-v2",           patch: { textDetector: "ImageTextDetectorV2" } },
    { label: "score-threshold-low",   patch: { scoreThreshold: 0.2, textThreshold: 0.2 } },
    { label: "refiner-off",           patch: { withRefiner: false } },
    { label: "save-helper-no",        patch: { includeSaveHelper: "no" } },
  ],

  pixelPhi: [
    { label: "default-vlm" },
    { label: "gpu-false",             patch: { useGPU: false } },
    { label: "non-vlm-v2",           patch: { px_ocrType: "non-vlm", px_ocrModel: "ImageToTextV2" } },
    { label: "non-vlm-v3",           patch: { px_ocrType: "non-vlm", px_ocrModel: "ImageToTextV3" } },
    { label: "compression-disabled", patch: { compressionMode: "disabled" } },
    { label: "compression-auto",     patch: { compressionMode: "auto", compressionThreshold: 2 } },
    { label: "frame-sampling-off",   patch: { enableFrameSampling: false } },
    { label: "frame-strategy-middle",patch: { frameStrategy: "MIDDLE", frameLimit: 3 } },
    { label: "scale-1-5x",           patch: { scale: 1.5 } },
    { label: "detector-v2",          patch: { px_ocrType: "non-vlm", px_ocrModel: "ImageToTextV2", textDetector: "ImageTextDetectorV2" } },
    { label: "vlm-npredict-512",     patch: { px_nPredict: 512, px_nCtx: 4096 } },
    { label: "save-helper-no",       patch: { includeSaveHelper: "no" } },
  ],

  encapsulatedPdf: [
    { label: "default-vlm" },
    { label: "non-vlm-v2",           patch: { px_ocrType: "non-vlm", px_ocrModel: "ImageToTextV2" } },
    { label: "non-vlm-v3",           patch: { px_ocrType: "non-vlm", px_ocrModel: "ImageToTextV3" } },
    { label: "tesseract",            patch: { px_ocrType: "tesseract", px_ocrModel: "ImageToText" } },
    { label: "resolution-150",       patch: { ep_resolution: 150 } },
    { label: "resolution-600",       patch: { ep_resolution: 600 } },
    { label: "gpu-false",            patch: { useGPU: false } },
    { label: "detector-v2",          patch: { px_ocrType: "non-vlm", px_ocrModel: "ImageToTextV2", textDetector: "ImageTextDetectorV2" } },
    { label: "save-helper-no",       patch: { includeSaveHelper: "no" } },
  ],

  metadata: [
    { label: "default-single-model" },
    { label: "two-models",           patch: { md_nerModels: ["zeroshot_ner_deid_subentity_merged_medium", "zeroshot_ner_deid_generic_docwise_medium"] } },
    { label: "large-model",          patch: { md_nerModels: ["zeroshot_ner_deid_subentity_merged_large"] } },
    { label: "non-medical-model",    patch: { md_nerModels: ["zeroshot_ner_deid_subentity_nonMedical_medium"] } },
    { label: "no-ner-models",        patch: { md_nerModels: [] } },
    { label: "remove-private-tags",  patch: { md_removePrivateTags: true } },
    { label: "group-strategy",       patch: { md_includeGroupStrategy: true } },
    { label: "save-helper-no",       patch: { includeSaveHelper: "no" } },
  ],

  strategyFile: [
    { label: "default" },
    { label: "delivery-download",    patch: { sf_delivery: "download" } },
    { label: "include-group",        patch: { sf_includeGroup: true, sf_groupRows: [{ prefix: "(0010,)", action: "remove" }] } },
    { label: "extra-tags",           patch: { sf_tagRows: [
      { tag: "(0010, 0010)", vr: "PN", name: "Patient Name", action: "replaceWithLiteral", option: "<REMOVED>" },
      { tag: "(0008, 0020)", vr: "DA", name: "Study Date",   action: "shiftDateByRandomNbOfDays", option: "" },
      { tag: "(0008, 0080)", vr: "LO", name: "Institution",  action: "delete", option: "" },
    ] } },
  ],
};

const WORKFLOW_MAP = {
  blanket:        { wf: workflowBlanket,       defaults: BLANKET_DEFAULTS },
  pixelPhi:       { wf: workflowPixelPhi,      defaults: PIXEL_PHI_DEFAULTS },
  encapsulatedPdf:{ wf: workflowEncapsulatedPdf, defaults: ENC_PDF_DEFAULTS },
  metadata:       { wf: workflowMetadata,      defaults: METADATA_DEFAULTS },
  strategyFile:   { wf: workflowStrategyFile,  defaults: STRATEGY_DEFAULTS },
};

// ── Generate and write ────────────────────────────────────────────────────────
const outRoot = path.join(__dirname, "test-output");
fs.mkdirSync(outRoot, { recursive: true });

let totalOk = 0, totalFail = 0;

for (const [wfKey, variants] of Object.entries(VARIANTS)) {
  const { wf, defaults } = WORKFLOW_MAP[wfKey];
  const wfDir = path.join(outRoot, wfKey);
  fs.mkdirSync(wfDir, { recursive: true });

  for (const v of variants) {
    const settings = Object.assign({}, defaults, v.patch || {});
    // Make settings available as state.settings for any stage that reads it
    sandbox.state.settings = settings;

    const outFile = path.join(wfDir, `${v.label}.py`);
    try {
      const code = wf.fullCode(settings);
      fs.writeFileSync(outFile, code, "utf8");
      console.log(`  OK   ${wfKey}/${v.label}`);
      totalOk++;
    } catch (err) {
      const msg = `# ERROR generating ${wfKey}/${v.label}\n# ${err.message}\n`;
      fs.writeFileSync(outFile, msg, "utf8");
      console.error(`  FAIL ${wfKey}/${v.label}: ${err.message}`);
      totalFail++;
    }
  }
}

console.log(`\nDone — ${totalOk} OK, ${totalFail} FAIL`);
console.log(`Output: ${outRoot}`);
