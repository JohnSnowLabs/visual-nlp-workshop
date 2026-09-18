// ─── Workflow registry ──────────────────────────────────────────────────────
// Assembled from individual workflow files. Add a new workflow by:
//   1. Creating workflows/<name>.js with  const workflowName = { name, description, steps: [...] };
//   2. Adding a <script> for it in index.html (before stages.js)
//   3. Adding the key here.

const workflows = {
  blanket:      workflowBlanket,
  pixelPhi:     workflowPixelPhi,
  metadata:     workflowMetadata,
  strategyFile: workflowStrategyFile,
  encapsulatedPdf: workflowEncapsulatedPdf,
};
