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


// ─── State ──────────────────────────────────────────────────────────────────
const STORAGE_KEY = "dicom_deid_state";

const state = {
  workflow: "blanket",
  stepIndex: 0,
  codeMode: "step",
  settings: {},
  completed: {},
  decided: {},
  understood: {},
  codeOpen: false,
  message: "",
  godMode: false,
  strategyFiles: {},
  strategyBuilderName: "",
};

function saveState() {
  try {
    syncStrategyRegistry();
    const payload = {
      workflow: state.workflow,
      stepIndex: state.stepIndex,
      codeMode: state.codeMode,
      settings: state.settings,
      completed: state.completed,
      decided: state.decided,
      understood: state.understood,
      codeOpen: state.codeOpen,
      strategyFiles: state.strategyFiles,
      strategyBuilderName: state.strategyBuilderName,
    };
    localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
  } catch (_) {}
}

function loadState() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return;
    const saved = JSON.parse(raw);
    Object.assign(state, saved);
    if (!state.understood) state.understood = {};
    if (!state.strategyFiles) state.strategyFiles = {};
    Object.keys(state.strategyFiles).forEach((name) => {
      const file = state.strategyFiles[name];
      if (!file) return;
      file.name = file.name || name;
      file.path = file.path || strategyPathFromName(name);
      file.delivery = file.delivery || "download";
    });
  } catch (_) {}
}

function syncStrategyRegistry() {
  if (!state.settings || !("sf_name" in state.settings)) return;
  const hasBuilderProgress = Boolean(state.strategyBuilderName)
    || Object.keys(state.completed || {}).some((k) => k.startsWith("strategyFile:"))
    || Object.keys(state.decided || {}).some((k) => k.startsWith("strategyFile:"));
  if (!hasBuilderProgress) return;
  state.strategyFiles = state.strategyFiles || {};
  const artifact = currentStrategyArtifact(state.settings);
  if (state.strategyBuilderName && state.strategyBuilderName !== artifact.name) {
    delete state.strategyFiles[state.strategyBuilderName];
  }
  state.strategyBuilderName = artifact.name;
  state.strategyFiles[artifact.name] = artifact;
}

// ─── Key helpers ─────────────────────────────────────────────────────────────
function currentWorkflow() { return workflows[state.workflow]; }
function currentStep()     { return currentWorkflow().steps[state.stepIndex]; }
function stepKey(i = state.stepIndex) { return `${state.workflow}:${i}`; }
function understandingKey(i, j) { return `${state.workflow}:${i}:${j}`; }
function decisionKey(k, i = state.stepIndex) { return `${state.workflow}:${i}:${k}`; }

function stageUnderstandingItems(step = currentStep()) {
  const mu = step.mustUnderstand;
  if (!mu) return [];
  return typeof mu === "function" ? mu(state.settings) : mu;
}

function isStageUnderstood(i = state.stepIndex) {
  const items = stageUnderstandingItems(currentWorkflow().steps[i]);
  if (items.length === 0) return true;
  return items.every((_, j) => state.understood[understandingKey(i, j)]);
}


function visibleControls(step) {
  return (step.controls || []).filter((c) => !c.when || c.when(state.settings));
}

// Display-only control types — not counted toward decisions, not gated
const DISPLAY_ONLY_TYPES = new Set(["reference-table", "workflow-overview"]);

function interactiveControls(step) {
  return visibleControls(step).filter((c) => !DISPLAY_ONLY_TYPES.has(c.type));
}

function controlOptions(control) {
  const options = typeof control.options === "function"
    ? control.options(state.settings, state)
    : (control.options || []);
  return options.map((o) =>
    typeof o === "string" ? { label: o, value: o, summary: "" } : o
  );
}

function isValuePresent(v) {
  if (Array.isArray(v)) return v.length > 0;
  return v !== undefined && v !== null && String(v).trim() !== "";
}

function isEffectivelyFilled(control) {
  if (isValuePresent(state.settings[control.key])) return true;
  // A placeholder counts as a usable value (user can Accept leaving the field blank)
  const ph = typeof control.placeholder === "function"
    ? control.placeholder(state.settings, state)
    : control.placeholder;
  return Boolean(ph);
}

function isControlDecided(k, i = state.stepIndex) {
  return Boolean(state.decided[decisionKey(k, i)]);
}

function missingRequiredControls(step) {
  return interactiveControls(step)
    .filter((c) => c.required)
    .filter((c) => !isEffectivelyFilled(c) || !isControlDecided(c.key));
}

function isStageConfirmed(i = state.stepIndex) {
  return Boolean(state.completed[stepKey(i)]);
}

function isStepVisible(i) {
  const step = currentWorkflow().steps[i];
  return !step.when || step.when(state.settings);
}

function canOpenStep(i) {
  for (let j = 0; j < i; j++) {
    if (!isStepVisible(j)) continue;          // hidden steps don't block
    if (!state.completed[stepKey(j)]) return false;
  }
  return true;
}

function allStagesConfirmed() {
  return currentWorkflow().steps.every((_, i) => !isStepVisible(i) || state.completed[stepKey(i)]);
}

function workflowSummary() {
  const steps = currentWorkflow().steps;
  const visible  = steps.filter((_, i) => isStepVisible(i));
  const nonOverview = visible.filter((s) => s.kind !== "Overview");
  const confirmed = nonOverview.filter((_, i) => state.completed[stepKey(steps.indexOf(nonOverview[i]))]).length;
  const required  = nonOverview.filter((s) => s.kind === "Required").length;
  const optional  = nonOverview.filter((s) => s.kind === "Optional").length;
  return { confirmed, total: nonOverview.length, required, optional };
}

function finalDicomColumn(settings) {
  return settings.bl_handleMetadata === true ? FIXED.metadataOutputCol : FIXED.pixelOutputCol;
}

function initSettings() {
  for (const wf of Object.values(workflows)) {
    for (const step of wf.steps) {
      for (const control of step.controls || []) {
        if (!(control.key in state.settings) && "value" in control) {
          state.settings[control.key] = typeof control.value === "function"
            ? control.value(state.settings, state)
            : control.value;
        }
      }
    }
  }
}

function workflowControlKeys(workflowKey) {
  const wf = workflows[workflowKey];
  if (!wf) return [];
  return wf.steps.flatMap((step) => (step.controls || []).map((control) => control.key));
}

function resetWorkflowMemory(workflowKey) {
  workflowControlKeys(workflowKey).forEach((key) => delete state.settings[key]);
  if (workflowKey === "strategyFile") {
    state.strategyFiles = {};
    state.strategyBuilderName = "";
  }
}

function invalidateFrom(i) {
  for (let j = i; j < currentWorkflow().steps.length; j++) delete state.completed[stepKey(j)];
}

function clearDecisionsFrom(i) {
  for (let j = i; j < currentWorkflow().steps.length; j++) {
    visibleControls(currentWorkflow().steps[j]).forEach((c) => {
      delete state.decided[decisionKey(c.key, j)];
    });
  }
}

function recordDecision(k) {
  state.decided[decisionKey(k)] = true;
  invalidateFrom(state.stepIndex);
  clearDecisionsFrom(state.stepIndex + 1);
  state.message = "Decision recorded. Confirm this stage when every decision is accepted.";
  saveState();
}

// ─── Syntax highlighting ─────────────────────────────────────────────────────
function highlight(raw) {
  const kwRe = /\b(from|import|def|class|return|if|else|elif|for|in|not|and|or|True|False|None|lambda|with|as|try|except|raise|yield|while|pass|break|continue|global|async|await)\b/g;
  const numRe = /\b(\d+\.?\d*)\b/g;

  const tokens = [];
  let pos = 0;
  while (pos < raw.length) {
    if (raw[pos] === "#") {
      const end = raw.indexOf("\n", pos);
      const text = end === -1 ? raw.slice(pos) : raw.slice(pos, end);
      tokens.push({ type: "comment", text });
      pos += text.length;
    } else if (raw.slice(pos, pos + 3) === '"""' || raw.slice(pos, pos + 3) === "'''") {
      const q = raw.slice(pos, pos + 3);
      const end = raw.indexOf(q, pos + 3);
      const text = end === -1 ? raw.slice(pos) : raw.slice(pos, end + 3);
      tokens.push({ type: "str", text });
      pos += text.length;
    } else if (raw[pos] === '"' || raw[pos] === "'") {
      const q = raw[pos];
      let e = pos + 1;
      while (e < raw.length && raw[e] !== q && raw[e] !== "\n") {
        if (raw[e] === "\\") e++;
        e++;
      }
      tokens.push({ type: "str", text: raw.slice(pos, e + 1) });
      pos = e + 1;
    } else {
      let e = pos + 1;
      while (e < raw.length && raw[e] !== "#" && raw[e] !== '"' && raw[e] !== "'") e++;
      tokens.push({ type: "plain", text: raw.slice(pos, e) });
      pos = e;
    }
  }

  return tokens.map((t) => {
    const esc = t.text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    if (t.type === "comment") return `<span class="tok-comment">${esc}</span>`;
    if (t.type === "str")     return `<span class="tok-str">${esc}</span>`;
    return esc
      .replace(kwRe, '<span class="tok-kw">$1</span>')
      .replace(numRe, '<span class="tok-num">$1</span>');
  }).join("");
}

// ─── Render helpers ───────────────────────────────────────────────────────────
function renderSteps() {
  const list = document.querySelector("#stepList");
  list.innerHTML = "";
  const { confirmed, total } = workflowSummary();
  const pct = total ? Math.round((confirmed / total) * 100) : 0;

  // Progress bar
  const prog = document.querySelector("#progressBar");
  prog.style.width = `${pct}%`;
  document.querySelector("#progressLabel").textContent =
    confirmed === total && total > 0 ? "Complete!" : `${confirmed} / ${total} confirmed`;

  currentWorkflow().steps.forEach((step, i) => {
    if (!isStepVisible(i)) return;
    const li = document.createElement("li");
    const btn = document.createElement("button");
    const locked = !canOpenStep(i);
    const done   = isStageConfirmed(i);

    btn.className = `step-button ${i === state.stepIndex ? "active" : ""} ${done ? "completed" : ""}`;
    btn.type = "button";
    btn.disabled = locked;
    btn.addEventListener("click", () => {
      if (locked) return;
      state.stepIndex = i;
      state.message = "";
      render();
    });

    btn.innerHTML = `
      <span class="step-number">${done ? "✓" : i + 1}</span>
      <span class="step-text">
        <span class="step-name">${step.title}</span>
        <span class="step-kind">${locked ? "Locked until prior stage is confirmed" : step.description}</span>
      </span>
      <span class="chip ${step.kind.toLowerCase()}">${step.kind}</span>
    `;
    li.appendChild(btn);
    list.appendChild(li);
  });

  const { required, optional } = workflowSummary();
  document.querySelector("#requiredCount").textContent =
    `${required} required${optional ? `, ${optional} optional` : ""}`;
}

function renderDetail() {
  const step  = currentStep();
  const badge = document.querySelector("#stepBadge");
  badge.textContent = step.kind;
  badge.className = `status-badge ${step.kind.toLowerCase()}`;
  document.querySelector("#stepTitle").textContent = step.title;
  document.querySelector("#stepDescription").textContent = step.description;
  const outputEl = document.querySelector("#stepOutput");
  if (step.output) { outputEl.textContent = "Output: " + step.output; outputEl.style.display = ""; }
  else { outputEl.textContent = ""; outputEl.style.display = "none"; }

  const controls = document.querySelector("#stageOptions");
  controls.innerHTML = "";
  const visible  = visibleControls(step);
  const interactive = interactiveControls(step);
  const accepted = interactive.filter((c) => isControlDecided(c.key)).length;
  document.querySelector("#decisionCount").textContent =
    interactive.length ? `${accepted}/${interactive.length} accepted` : "No config";


  if (!interactive.length && !visible.length) {
    const p = document.createElement("p");
    p.className = "no-config";
    p.textContent = "No result-changing configuration here. Review the checkpoint, then confirm.";
    controls.appendChild(p);
  } else {
    visible.forEach((c) => controls.appendChild(createControl(c)));
  }

  renderUnderstandingGate();
  renderDecisionGuide();
}


function renderUnderstandingGate() {
  const gate  = document.querySelector("#understandingGate");
  const step  = currentStep();
  const items = stageUnderstandingItems(step);
  gate.innerHTML = "";
  if (items.length === 0) return;

  const done  = items.filter((_, j) => state.understood[understandingKey(state.stepIndex, j)]).length;
  const hasControls = interactiveControls(step).length > 0;

  gate.innerHTML = `
    <div class="gate-heading">
      <span>${hasControls ? "Before you choose" : "Stage review"}</span>
      <strong>${done}/${items.length}</strong>
    </div>
  `;

  const list = document.createElement("div");
  list.className = "understanding-list";
  items.forEach((item, j) => {
    const label = document.createElement("label");
    label.className = "understanding-item";
    const input = document.createElement("input");
    input.type = "checkbox";
    input.checked = Boolean(state.understood[understandingKey(state.stepIndex, j)]);
    input.addEventListener("change", (e) => {
      state.understood[understandingKey(state.stepIndex, j)] = e.target.checked;
      invalidateFrom(state.stepIndex);
      state.message = isStageUnderstood()
        ? hasControls ? "Now make and accept each configuration decision." : "Review complete. Confirm this stage to continue."
        : "Complete the review checkpoint before continuing.";
      saveState();
      render();
    });
    label.appendChild(input);
    label.append(item);
    list.appendChild(label);
  });
  gate.appendChild(list);
}

function renderDecisionGuide() {
  const guide = document.querySelector("#decisionGuide");
  guide.innerHTML = "";
  if (state.clearChoiceSummary) return;
  const step  = currentStep();
  const visible = interactiveControls(step);
  const anyDecided = visible.some((c) => isControlDecided(c.key));
  if (!anyDecided && !isStageConfirmed()) return;
  const rawSummary = step.decisionSummary ? step.decisionSummary(state.settings) : [];
  const summary = Array.isArray(rawSummary) ? rawSummary : (rawSummary ? [rawSummary] : []);
  const impacts = visible.map((c) => {
    if (c.impact) return c.impact(state.settings[c.key], state.settings);
    const opt = controlOptions(c).find((o) => o.value === state.settings[c.key]);
    return opt && opt.summary ? `${c.label}: ${opt.summary}` : "";
  }).filter(Boolean);

  const ul = document.createElement("ul");
  ul.className = "impact-list";
  [...summary, ...impacts].forEach((item) => {
    const li = document.createElement("li");
    li.textContent = item;
    ul.appendChild(li);
  });
  guide.appendChild(ul);
}

function updateControlImpact(node, control) {
  const v = state.settings[control.key];
  if (control.impact) { node.textContent = control.impact(v, state.settings); return; }
  const opt = controlOptions(control).find((o) => o.value === v);
  node.textContent = opt && opt.summary ? opt.summary : "This setting changes the generated pipeline.";
}

function createControl(control) {
  const wrapper  = document.createElement("div");
  wrapper.className = "decision-row";
  const locked   = !isStageUnderstood();
  const accepted = isControlDecided(control.key);
  if (accepted) wrapper.classList.add("accepted");
  if (locked)   wrapper.classList.add("locked");

  // ── workflow-overview: rendered as a summary card, no Accept button ───────
  if (control.type === "workflow-overview") {
    wrapper.classList.add("wf-overview-wrapper");
    const d = control.data || {};

    const makeSection = (icon, title, items) => {
      if (!items || items.length === 0) return null;
      const sec = document.createElement("div");
      sec.className = "wfo-section";
      const hd = document.createElement("div");
      hd.className = "wfo-section-head";
      hd.innerHTML = `<span class="wfo-icon">${icon}</span><span class="wfo-section-title">${title}</span>`;
      sec.appendChild(hd);
      const ul = document.createElement("ul");
      ul.className = "wfo-list";
      items.forEach((item) => {
        const li = document.createElement("li");
        li.textContent = item;
        ul.appendChild(li);
      });
      sec.appendChild(ul);
      return sec;
    };

    const grid = document.createElement("div");
    grid.className = "wfo-grid";

    [
      makeSection("📋", "What it does", d.what),
      makeSection("📤", "Outputs", d.outputs),
      makeSection("⚙️", "Prerequisites", d.prerequisites),
      makeSection("💡", "When to use", d.whenToUse),
    ].forEach((sec) => { if (sec) grid.appendChild(sec); });

    wrapper.appendChild(grid);
    return wrapper;
  }

  // ── reference-table: rendered as a read-only table, no Accept button ───────
  if (control.type === "reference-table") {
    wrapper.classList.add("ref-table-wrapper");
    const table = document.createElement("table");
    table.className = "ref-table";

    const thead = document.createElement("thead");
    const hRow  = document.createElement("tr");
    ["Action", "VRs", "Option", "Effect"].forEach((h) => {
      const th = document.createElement("th");
      th.textContent = h;
      hRow.appendChild(th);
    });
    thead.appendChild(hRow);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    (control.data || []).forEach((row) => {
      const tr = document.createElement("tr");
      if (row.category) {
        tr.className = "ref-category-row";
        const td = document.createElement("td");
        td.colSpan = 4;
        td.textContent = row.label;
        tr.appendChild(td);
      } else {
        const tdAction = document.createElement("td");
        const code = document.createElement("code");
        code.textContent = row.action;
        tdAction.appendChild(code);

        const tdVrs    = document.createElement("td");
        tdVrs.textContent = row.vrs || "All";

        const tdOpt    = document.createElement("td");
        tdOpt.textContent = row.option || "—";

        const tdEffect = document.createElement("td");
        tdEffect.textContent = row.effect;

        tr.appendChild(tdAction);
        tr.appendChild(tdVrs);
        tr.appendChild(tdOpt);
        tr.appendChild(tdEffect);
      }
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    wrapper.appendChild(table);
    return wrapper;
  }

  // Meta column
  const meta = document.createElement("div");
  meta.className = "decision-meta";
  const lbl = document.createElement("label");
  lbl.textContent = control.label;
  lbl.htmlFor = `ctrl-${control.key}`;
  meta.appendChild(lbl);
  if (control.help) {
    const help = document.createElement("p");
    help.textContent = control.help;
    meta.appendChild(help);
  }

  // Input column
  const inputArea = document.createElement("div");
  inputArea.className = "decision-control";
  const impact = document.createElement("p");
  impact.className = "decision-impact";
  updateControlImpact(impact, control);

  if (control.type === "select") {
    const sel = document.createElement("select");
    sel.id = `ctrl-${control.key}`;
    sel.disabled = locked;
    controlOptions(control).forEach((opt) => {
      const item = document.createElement("option");
      item.value = opt.value;
      item.textContent = opt.label;
      item.selected = state.settings[control.key] === opt.value;
      sel.appendChild(item);
    });
    sel.addEventListener("change", (e) => {
      state.settings[control.key] = e.target.value;
      recordDecision(control.key);
      render();
    });
    inputArea.appendChild(sel);
  }

  if (control.type === "range") {
    const valSpan = document.createElement("span");
    valSpan.className = "field-value";
    valSpan.textContent = state.settings[control.key];
    const inp = document.createElement("input");
    inp.id = `ctrl-${control.key}`;
    inp.type = "range";
    inp.min = control.min; inp.max = control.max;
    inp.step = control.step || 1;
    inp.value = state.settings[control.key];
    inp.disabled = locked;
    inp.addEventListener("input", (e) => {
      state.settings[control.key] = Number(e.target.value);
      valSpan.textContent = state.settings[control.key];
      recordDecision(control.key);
      renderCode(); renderDecisionGuide();
      updateControlImpact(impact, control);
      renderNav();
    });
    const rangeWrap = document.createElement("div");
    rangeWrap.className = "range-control";
    rangeWrap.appendChild(inp);
    rangeWrap.appendChild(valSpan);
    inputArea.appendChild(rangeWrap);
  }

  if (control.type === "choice") {
    const group = document.createElement("fieldset");
    group.className = "choice-group";
    controlOptions(control).forEach((opt) => {
      const lbl2 = document.createElement("label");
      lbl2.className = "choice-option";
      const inp = document.createElement("input");
      inp.type = "radio"; inp.name = control.key; inp.value = opt.value;
      inp.checked = state.settings[control.key] === opt.value;
      inp.disabled = locked;
      inp.addEventListener("change", (e) => {
        const raw = e.target.value;
        const matched = controlOptions(control).find((o) => String(o.value) === raw);
        state.settings[control.key] = matched ? matched.value : raw;
        if (control.onSelect) control.onSelect(state.settings[control.key], state.settings);
        recordDecision(control.key);
        render();
      });
      lbl2.appendChild(inp);
      lbl2.append(opt.label);
      if (opt.summary) {
        const hint = document.createElement("span");
        hint.className = "option-summary"; hint.textContent = opt.summary;
        lbl2.appendChild(hint);
      }
      group.appendChild(lbl2);
    });
    inputArea.appendChild(group);
  }

  if (control.type === "toggle") {
    const row = document.createElement("label");
    row.className = "toggle-control";
    const inp = document.createElement("input");
    inp.id = `ctrl-${control.key}`; inp.type = "checkbox";
    inp.checked = Boolean(state.settings[control.key]);
    inp.disabled = locked;
    inp.addEventListener("change", (e) => {
      state.settings[control.key] = e.target.checked;
      recordDecision(control.key);
      render();
    });
    row.appendChild(inp); row.append(control.label);
    inputArea.appendChild(row);
  }


  if (control.type === "multiselect") {
    if (!Array.isArray(state.settings[control.key])) {
      state.settings[control.key] = Array.isArray(control.value) ? [...control.value] : [];
    }
    const group = document.createElement("fieldset");
    group.className = "choice-group multiselect-group";
    controlOptions(control).forEach((opt) => {
      const lbl2 = document.createElement("label");
      lbl2.className = "choice-option";
      const inp = document.createElement("input");
      inp.type = "checkbox"; inp.name = control.key; inp.value = opt.value;
      inp.checked = state.settings[control.key].includes(opt.value);
      inp.disabled = locked;
      inp.addEventListener("change", (e) => {
        let arr = Array.isArray(state.settings[control.key]) ? [...state.settings[control.key]] : [];
        if (e.target.checked) {
          if (!arr.includes(opt.value)) arr.push(opt.value);
        } else {
          arr = arr.filter((v) => v !== opt.value);
        }
        state.settings[control.key] = arr;
        recordDecision(control.key);
        render();
      });
      lbl2.appendChild(inp);
      lbl2.append(opt.label);
      if (opt.summary) {
        const hint = document.createElement("span");
        hint.className = "option-summary"; hint.textContent = opt.summary;
        lbl2.appendChild(hint);
      }
      group.appendChild(lbl2);
    });
    inputArea.appendChild(group);
  }


  if (control.type === "tag-table") {
  // ── DICOM de-id preset profiles ───────────────────────────────────────────
  const PRESET_PROFILES = {
    "HIPAA Safe Harbor": [
      { tag: "(0010, 0010)", vr: "PN",  name: "Patient Name",                  action: "replaceWithRandomName",      option: "" },
      { tag: "(0010, 0020)", vr: "LO",  name: "Patient ID",                    action: "hashId",                     option: "" },
      { tag: "(0010, 0030)", vr: "DA",  name: "Patient Birth Date",             action: "remove",                     option: "" },
      { tag: "(0010, 0032)", vr: "TM",  name: "Patient Birth Time",             action: "remove",                     option: "" },
      { tag: "(0010, 0040)", vr: "CS",  name: "Patient Sex",                   action: "remove",                     option: "" },
      { tag: "(0010, 1010)", vr: "AS",  name: "Patient Age",                   action: "remove",                     option: "" },
      { tag: "(0010, 1020)", vr: "DS",  name: "Patient Size",                  action: "remove",                     option: "" },
      { tag: "(0010, 1030)", vr: "DS",  name: "Patient Weight",                action: "remove",                     option: "" },
      { tag: "(0010, 1040)", vr: "LO",  name: "Patient Address",               action: "remove",                     option: "" },
      { tag: "(0010, 1060)", vr: "PN",  name: "Patient Mother Maiden Name",    action: "remove",                     option: "" },
      { tag: "(0010, 1000)", vr: "LO",  name: "Other Patient IDs",             action: "hashId",                     option: "" },
      { tag: "(0010, 1001)", vr: "PN",  name: "Other Patient Names",           action: "replaceWithRandomName",      option: "" },
      { tag: "(0010, 2154)", vr: "SH",  name: "Patient Telephone Numbers",     action: "remove",                     option: "" },
      { tag: "(0010, 2160)", vr: "SH",  name: "Ethnic Group",                  action: "remove",                     option: "" },
      { tag: "(0010, 2180)", vr: "SH",  name: "Occupation",                    action: "remove",                     option: "" },
      { tag: "(0010, 21B0)", vr: "LT",  name: "Additional Patient History",    action: "remove",                     option: "" },
      { tag: "(0010, 4000)", vr: "LT",  name: "Patient Comments",              action: "remove",                     option: "" },
      { tag: "(0008, 0020)", vr: "DA",  name: "Study Date",                    action: "shiftDateByRandomNbOfDays",  option: "" },
      { tag: "(0008, 0021)", vr: "DA",  name: "Series Date",                   action: "shiftDateByRandomNbOfDays",  option: "" },
      { tag: "(0008, 0022)", vr: "DA",  name: "Acquisition Date",              action: "shiftDateByRandomNbOfDays",  option: "" },
      { tag: "(0008, 0023)", vr: "DA",  name: "Content Date",                  action: "shiftDateByRandomNbOfDays",  option: "" },
      { tag: "(0008, 0030)", vr: "TM",  name: "Study Time",                    action: "remove",                     option: "" },
      { tag: "(0008, 0031)", vr: "TM",  name: "Series Time",                   action: "remove",                     option: "" },
      { tag: "(0008, 0032)", vr: "TM",  name: "Acquisition Time",              action: "remove",                     option: "" },
      { tag: "(0008, 0033)", vr: "TM",  name: "Content Time",                  action: "remove",                     option: "" },
      { tag: "(0008, 0050)", vr: "SH",  name: "Accession Number",              action: "hashId",                     option: "" },
      { tag: "(0008, 0018)", vr: "UI",  name: "SOP Instance UID",              action: "hashId",                     option: "" },
      { tag: "(0008, 0080)", vr: "LO",  name: "Institution Name",              action: "remove",                     option: "" },
      { tag: "(0008, 0081)", vr: "ST",  name: "Institution Address",           action: "remove",                     option: "" },
      { tag: "(0008, 0090)", vr: "PN",  name: "Referring Physician Name",      action: "replaceWithRandomName",      option: "" },
      { tag: "(0008, 0096)", vr: "SQ",  name: "Referring Physician ID Seq",    action: "remove",                     option: "" },
      { tag: "(0008, 1010)", vr: "SH",  name: "Station Name",                  action: "remove",                     option: "" },
      { tag: "(0008, 1030)", vr: "LO",  name: "Study Description",             action: "remove",                     option: "" },
      { tag: "(0008, 103E)", vr: "LO",  name: "Series Description",            action: "remove",                     option: "" },
      { tag: "(0008, 1048)", vr: "PN",  name: "Physician(s) of Record",        action: "replaceWithRandomName",      option: "" },
      { tag: "(0008, 1050)", vr: "PN",  name: "Performing Physician Name",     action: "replaceWithRandomName",      option: "" },
      { tag: "(0008, 1070)", vr: "PN",  name: "Operators Name",               action: "replaceWithRandomName",      option: "" },
      { tag: "(0018, 1000)", vr: "LO",  name: "Device Serial Number",          action: "remove",                     option: "" },
      { tag: "(0018, 1030)", vr: "LO",  name: "Protocol Name",                 action: "remove",                     option: "" },
      { tag: "(0020, 000D)", vr: "UI",  name: "Study Instance UID",            action: "hashId",                     option: "" },
      { tag: "(0020, 000E)", vr: "UI",  name: "Series Instance UID",           action: "hashId",                     option: "" },
      { tag: "(0020, 0010)", vr: "SH",  name: "Study ID",                      action: "hashId",                     option: "" },
      { tag: "(0020, 0052)", vr: "UI",  name: "Frame of Reference UID",        action: "hashId",                     option: "" },
      { tag: "(0040, 0275)", vr: "SQ",  name: "Request Attributes Sequence",   action: "remove",                     option: "" },
    ],
    "DICOM PS3.15 Basic": [
      { tag: "(0008, 0014)", vr: "UI",  name: "Instance Creator UID",          action: "hashId",                     option: "" },
      { tag: "(0008, 0018)", vr: "UI",  name: "SOP Instance UID",              action: "hashId",                     option: "" },
      { tag: "(0008, 0020)", vr: "DA",  name: "Study Date",                    action: "shiftDateByRandomNbOfDays",  option: "" },
      { tag: "(0008, 0021)", vr: "DA",  name: "Series Date",                   action: "shiftDateByRandomNbOfDays",  option: "" },
      { tag: "(0008, 0022)", vr: "DA",  name: "Acquisition Date",              action: "shiftDateByRandomNbOfDays",  option: "" },
      { tag: "(0008, 0023)", vr: "DA",  name: "Content Date",                  action: "shiftDateByRandomNbOfDays",  option: "" },
      { tag: "(0008, 0030)", vr: "TM",  name: "Study Time",                    action: "remove",                     option: "" },
      { tag: "(0008, 0031)", vr: "TM",  name: "Series Time",                   action: "remove",                     option: "" },
      { tag: "(0008, 0032)", vr: "TM",  name: "Acquisition Time",              action: "remove",                     option: "" },
      { tag: "(0008, 0033)", vr: "TM",  name: "Content Time",                  action: "remove",                     option: "" },
      { tag: "(0008, 0050)", vr: "SH",  name: "Accession Number",              action: "hashId",                     option: "" },
      { tag: "(0008, 0080)", vr: "LO",  name: "Institution Name",              action: "remove",                     option: "" },
      { tag: "(0008, 0081)", vr: "ST",  name: "Institution Address",           action: "remove",                     option: "" },
      { tag: "(0008, 0090)", vr: "PN",  name: "Referring Physician Name",      action: "replaceWithRandomName",      option: "" },
      { tag: "(0008, 0096)", vr: "SQ",  name: "Referring Physician ID Seq",    action: "remove",                     option: "" },
      { tag: "(0008, 1010)", vr: "SH",  name: "Station Name",                  action: "remove",                     option: "" },
      { tag: "(0008, 1030)", vr: "LO",  name: "Study Description",             action: "remove",                     option: "" },
      { tag: "(0008, 1040)", vr: "LO",  name: "Institutional Department Name", action: "remove",                     option: "" },
      { tag: "(0008, 1048)", vr: "PN",  name: "Physician(s) of Record",        action: "replaceWithRandomName",      option: "" },
      { tag: "(0008, 1050)", vr: "PN",  name: "Performing Physician Name",     action: "replaceWithRandomName",      option: "" },
      { tag: "(0008, 1060)", vr: "PN",  name: "Name of Physician Reading Study", action: "replaceWithRandomName",    option: "" },
      { tag: "(0008, 1070)", vr: "PN",  name: "Operators Name",               action: "replaceWithRandomName",      option: "" },
      { tag: "(0008, 1155)", vr: "UI",  name: "Referenced SOP Instance UID",   action: "hashId",                     option: "" },
      { tag: "(0008, 103E)", vr: "LO",  name: "Series Description",            action: "remove",                     option: "" },
      { tag: "(0010, 0010)", vr: "PN",  name: "Patient Name",                  action: "replaceWithRandomName",      option: "" },
      { tag: "(0010, 0020)", vr: "LO",  name: "Patient ID",                    action: "hashId",                     option: "" },
      { tag: "(0010, 0030)", vr: "DA",  name: "Patient Birth Date",             action: "remove",                     option: "" },
      { tag: "(0010, 0040)", vr: "CS",  name: "Patient Sex",                   action: "remove",                     option: "" },
      { tag: "(0010, 1010)", vr: "AS",  name: "Patient Age",                   action: "remove",                     option: "" },
      { tag: "(0010, 1020)", vr: "DS",  name: "Patient Size",                  action: "remove",                     option: "" },
      { tag: "(0010, 1030)", vr: "DS",  name: "Patient Weight",                action: "remove",                     option: "" },
      { tag: "(0010, 1040)", vr: "LO",  name: "Patient Address",               action: "remove",                     option: "" },
      { tag: "(0010, 2154)", vr: "SH",  name: "Patient Telephone Numbers",     action: "remove",                     option: "" },
      { tag: "(0010, 2160)", vr: "SH",  name: "Ethnic Group",                  action: "remove",                     option: "" },
      { tag: "(0010, 4000)", vr: "LT",  name: "Patient Comments",              action: "remove",                     option: "" },
      { tag: "(0018, 1000)", vr: "LO",  name: "Device Serial Number",          action: "remove",                     option: "" },
      { tag: "(0018, 1020)", vr: "LO",  name: "Software Versions",             action: "remove",                     option: "" },
      { tag: "(0018, 1030)", vr: "LO",  name: "Protocol Name",                 action: "remove",                     option: "" },
      { tag: "(0020, 000D)", vr: "UI",  name: "Study Instance UID",            action: "hashId",                     option: "" },
      { tag: "(0020, 000E)", vr: "UI",  name: "Series Instance UID",           action: "hashId",                     option: "" },
      { tag: "(0020, 0010)", vr: "SH",  name: "Study ID",                      action: "hashId",                     option: "" },
      { tag: "(0020, 0052)", vr: "UI",  name: "Frame of Reference UID",        action: "hashId",                     option: "" },
      { tag: "(0020, 0200)", vr: "UI",  name: "Synchronization Frame UID",     action: "hashId",                     option: "" },
      { tag: "(0040, A124)", vr: "UI",  name: "UID",                           action: "hashId",                     option: "" },
      { tag: "(0088, 0140)", vr: "UI",  name: "Storage Media File-set UID",    action: "hashId",                     option: "" },
    ],
    "Minimal": [
      { tag: "(0010, 0010)", vr: "PN",  name: "Patient Name",                  action: "replaceWithLiteral",         option: "<REMOVED>" },
      { tag: "(0010, 0020)", vr: "LO",  name: "Patient ID",                    action: "hashId",                     option: "" },
      { tag: "(0010, 0030)", vr: "DA",  name: "Patient Birth Date",             action: "remove",                     option: "" },
      { tag: "(0008, 0020)", vr: "DA",  name: "Study Date",                    action: "shiftDateByRandomNbOfDays",  option: "" },
      { tag: "(0008, 0050)", vr: "SH",  name: "Accession Number",              action: "hashId",                     option: "" },
      { tag: "(0020, 000D)", vr: "UI",  name: "Study Instance UID",            action: "hashId",                     option: "" },
    ],
  };


    const VR_ACTIONS = {
      PN:  ["replaceWithLiteral", "replaceWithRandomName", "remove", "delete", "ensureTagExists"],
      LO:  ["hashId", "patientHashId", "remove", "delete", "replaceWithLiteral", "ensureTagExists"],
      SH:  ["hashId", "patientHashId", "remove", "delete", "replaceWithLiteral"],
      UI:  ["hashId", "remove", "delete"],
      DA:  ["remove", "shiftDateByRandomNbOfDays", "shiftDateByFixedNbOfDays", "delete"],
      DT:  ["remove", "shiftDateByRandomNbOfDays", "shiftDateByFixedNbOfDays", "delete"],
      TM:  ["shiftTimeByRandom", "remove", "delete"],
      AS:  ["shiftAgeByRandom", "capAgeAt99IfOver90", "remove", "delete"],
      CS:  ["remove", "delete", "replaceWithLiteral"],
      IS:  ["remove", "delete", "replaceWithLiteral", "hashId"],
      DS:  ["remove", "delete", "replaceWithLiteral"],
      LT:  ["remove", "delete", "replaceWithLiteral"],
      ST:  ["remove", "delete", "replaceWithLiteral"],
      UT:  ["remove", "delete", "replaceWithLiteral"],
      AE:  ["remove", "delete", "replaceWithLiteral"],
      US:  ["remove", "delete", "replaceWithLiteral"],
      OB:  ["remove", "delete"],
      OW:  ["remove", "delete"],
      UN:  ["remove", "delete"],
      SQ:  ["remove", "delete"],
      OF:  ["remove", "delete"],
      OD:  ["remove", "delete"],
      OL:  ["remove", "delete"],
      FD:  ["remove", "delete", "replaceWithLiteral"],
      FL:  ["remove", "delete", "replaceWithLiteral"],
      SL:  ["remove", "delete", "replaceWithLiteral"],
      UL:  ["remove", "delete", "replaceWithLiteral"],
      SS:  ["remove", "delete", "replaceWithLiteral"],
      AT:  ["remove", "delete"],
    };
    const ALL_ACTIONS = [
      "hashId","patientHashId","replaceWithLiteral","replaceWithRandomName","remove","delete",
      "ensureTagExists","shiftDateByRandomNbOfDays","shiftDateByFixedNbOfDays",
      "shiftTimeByRandom","shiftUnixTimeStampRandom","shiftAgeByRandom","capAgeAt99IfOver90",
    ];
    const OPTION_NEEDED = new Set(["replaceWithLiteral","shiftDateByFixedNbOfDays"]);
    const ALL_VRS = ["PN","LO","SH","UI","DA","DT","TM","AS","CS","IS","DS","LT","ST","UT","AE","US","OB","OW","UN","SQ","OF","OD","OL","FD","SL","UL","SS","FL","AT"];

    if (!Array.isArray(state.settings[control.key])) {
      state.settings[control.key] = Array.isArray(control.value) ? [...control.value] : [];
    }
    const rows = state.settings[control.key];
    let updateTagActionState = () => {};

    const saveRows = () => {
      state.settings[control.key] = [...rows];
      saveState();
      render();
      updateTagActionState();
    };

    const table = document.createElement("table");
    table.className = "tag-table";

    // Header
    const thead = document.createElement("thead");
    thead.innerHTML = `<tr>
      <th>Tag</th><th>VR</th><th>Name</th><th>Action</th><th>Option</th><th></th>
    </tr>`;
    table.appendChild(thead);

    const tbody = document.createElement("tbody");

    const renderTableRows = () => {
      tbody.innerHTML = "";
      rows.forEach((row, idx) => {
        const tr = document.createElement("tr");

        // Tag cell
        const tdTag = document.createElement("td");
        const inpTag = document.createElement("input");
        inpTag.type = "text"; inpTag.value = row.tag || "";
        inpTag.placeholder = "(0010, 0010)";
        inpTag.disabled = locked;
        inpTag.addEventListener("change", (e) => { rows[idx].tag = e.target.value; saveRows(); });
        tdTag.appendChild(inpTag); tr.appendChild(tdTag);

        // VR cell
        const tdVr = document.createElement("td");
        const selVr = document.createElement("select");
        selVr.disabled = locked;
        ALL_VRS.forEach((vr) => {
          const opt = document.createElement("option");
          opt.value = vr; opt.textContent = vr;
          opt.selected = row.vr === vr;
          selVr.appendChild(opt);
        });
        selVr.addEventListener("change", (e) => {
          rows[idx].vr = e.target.value;
          const validActions = VR_ACTIONS[e.target.value] || ALL_ACTIONS;
          if (!validActions.includes(rows[idx].action)) rows[idx].action = validActions[0];
          saveRows(); renderTableRows();
        });
        tdVr.appendChild(selVr); tr.appendChild(tdVr);

        // Name cell
        const tdName = document.createElement("td");
        const inpName = document.createElement("input");
        inpName.type = "text"; inpName.value = row.name || "";
        inpName.placeholder = "Tag name";
        inpName.disabled = locked;
        inpName.addEventListener("change", (e) => { rows[idx].name = e.target.value; saveRows(); });
        tdName.appendChild(inpName); tr.appendChild(tdName);

        // Action cell
        const tdAction = document.createElement("td");
        const selAction = document.createElement("select");
        selAction.disabled = locked;
        const validActions = VR_ACTIONS[row.vr] || ALL_ACTIONS;
        validActions.forEach((a) => {
          const opt = document.createElement("option");
          opt.value = a; opt.textContent = a;
          opt.selected = row.action === a;
          selAction.appendChild(opt);
        });
        selAction.addEventListener("change", (e) => {
          rows[idx].action = e.target.value;
          if (!OPTION_NEEDED.has(e.target.value)) rows[idx].option = "";
          saveRows(); renderTableRows();
        });
        tdAction.appendChild(selAction); tr.appendChild(tdAction);

        // Option cell
        const tdOpt = document.createElement("td");
        if (OPTION_NEEDED.has(row.action)) {
          const inpOpt = document.createElement("input");
          inpOpt.type = "text"; inpOpt.value = row.option || "";
          inpOpt.placeholder = row.action === "replaceWithLiteral" ? "<REMOVED>" : "e.g. -30";
          inpOpt.disabled = locked;
          inpOpt.addEventListener("change", (e) => { rows[idx].option = e.target.value; saveRows(); });
          tdOpt.appendChild(inpOpt);
        } else {
          tdOpt.textContent = "—";
        }
        tr.appendChild(tdOpt);

        // Delete button
        const tdDel = document.createElement("td");
        const btnDel = document.createElement("button");
        btnDel.type = "button"; btnDel.className = "tag-row-del"; btnDel.textContent = "✕";
        btnDel.disabled = locked;
        btnDel.addEventListener("click", () => { rows.splice(idx, 1); saveRows(); renderTableRows(); });
        tdDel.appendChild(btnDel); tr.appendChild(tdDel);

        tbody.appendChild(tr);
      });
    };

    renderTableRows();
    table.appendChild(tbody);

    // Add row button
    const addBtn = document.createElement("button");
    addBtn.type = "button"; addBtn.className = "tag-row-add"; addBtn.textContent = "+ Add tag";
    addBtn.disabled = locked;
    addBtn.addEventListener("click", () => {
      rows.push({ tag: "", vr: "LO", name: "", action: "remove", option: "" });
      saveRows(); renderTableRows();
    });

    const uploadInput = document.createElement("input");
    uploadInput.type = "file";
    uploadInput.accept = ".csv,text/csv";
    uploadInput.hidden = true;

    const uploadBtn = document.createElement("button");
    uploadBtn.type = "button";
    uploadBtn.className = "tag-row-upload";
    uploadBtn.textContent = "Upload CSV";
    uploadBtn.disabled = locked;
    uploadBtn.addEventListener("click", () => uploadInput.click());
    uploadInput.addEventListener("change", async () => {
      const file = uploadInput.files && uploadInput.files[0];
      uploadInput.value = "";
      if (!file) return;
      const uploadedRows = tagRowsFromCsv(await file.text());
      if (!uploadedRows.length) {
        state.message = "No tag rows found in the uploaded CSV.";
        saveState();
        renderNav();
        return;
      }
      rows.length = 0;
      uploadedRows.forEach((r) => rows.push(r));
      saveRows();
      renderTableRows();
    });

    const clearBtn = document.createElement("button");
    clearBtn.type = "button";
    clearBtn.className = "tag-row-clear";
    clearBtn.textContent = "Clear";
    clearBtn.disabled = locked || rows.length === 0;
    clearBtn.addEventListener("click", () => {
      rows.length = 0;
      state.settings[control.key] = [];
      delete state.decided[decisionKey(control.key)];
      invalidateFrom(state.stepIndex);
      state.message = "Tag rules cleared.";
      saveState();
      render();
    });
    updateTagActionState = () => {
      clearBtn.disabled = locked || rows.length === 0;
    };

    // Preset picker bar
    const presetBar = document.createElement("div");
    presetBar.className = "preset-bar";
    const presetLabel = document.createElement("span");
    presetLabel.className = "preset-bar-label";
    presetLabel.textContent = "Load preset:";
    presetBar.appendChild(presetLabel);
    Object.keys(PRESET_PROFILES).forEach((profileName) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "preset-chip";
      btn.textContent = profileName;
      btn.disabled = locked;
      btn.title = `Load ${profileName} tags (replaces current rows)`;
      btn.addEventListener("click", () => {
        if (rows.length > 0 && !confirm(`Replace current ${rows.length} row(s) with the "${profileName}" preset?`)) return;
        rows.length = 0;
        PRESET_PROFILES[profileName].forEach((r) => rows.push({ ...r }));
        saveRows();
        renderTableRows();
      });
      presetBar.appendChild(btn);
    });
    // Merge button — appends without replacing
    const mergeBtn = document.createElement("button");
    mergeBtn.type = "button";
    mergeBtn.className = "preset-chip preset-chip-merge";
    mergeBtn.textContent = "＋ Append preset…";
    mergeBtn.disabled = locked;
    mergeBtn.title = "Add a preset's rows to the current list (without replacing)";
    const mergeSelect = document.createElement("select");
    mergeSelect.className = "preset-merge-select";
    mergeSelect.innerHTML = `<option value="">— choose —</option>` + Object.keys(PRESET_PROFILES).map((n) => `<option value="${n}">${n}</option>`).join("");
    mergeSelect.addEventListener("change", () => {
      const chosen = mergeSelect.value;
      if (!chosen) return;
      const existing = new Set(rows.map((r) => r.tag));
      PRESET_PROFILES[chosen].forEach((r) => { if (!existing.has(r.tag)) rows.push({ ...r }); });
      mergeSelect.value = "";
      saveRows();
      renderTableRows();
    });
    presetBar.appendChild(mergeSelect);
    const tableActions = document.createElement("div");
    tableActions.className = "tag-table-actions";
    tableActions.appendChild(addBtn);
    tableActions.appendChild(uploadBtn);
    tableActions.appendChild(clearBtn);
    tableActions.appendChild(uploadInput);
    updateTagActionState();
    inputArea.appendChild(tableActions);
    inputArea.appendChild(table);
    inputArea.insertBefore(presetBar, table);
  }


  // ── text control ─────────────────────────────────────────────────────────
  if (control.type === "text") {
    const inp = document.createElement("input");
    inp.type = "text";
    inp.className = "text-control-input";
    inp.value = state.settings[control.key] ?? control.value ?? "";
    inp.disabled = locked;
    inp.placeholder = typeof control.placeholder === "function"
      ? control.placeholder(state.settings, state)
      : (control.placeholder || "");
    inp.addEventListener("input", () => {
      state.settings[control.key] = inp.value;
      updateControlImpact(impact, control);
      saveState();
      render();
    });
    inputArea.appendChild(inp);
  }

  // ── group-table control ───────────────────────────────────────────────────
  if (control.type === "group-table") {
    const GROUP_ACTIONS = ["remove", "delete"];
    const DEFAULT_GROUP_ROWS = [
      { prefix: "(6,)", action: "remove" },
      { prefix: "(80,)", action: "remove" },
    ];

    if (!state.settings[control.key]) {
      state.settings[control.key] = DEFAULT_GROUP_ROWS.map((r) => ({ ...r }));
      saveState();
    }

    let gRows = state.settings[control.key];
    let updateGroupActionState = () => {};

    const saveGRows = () => {
      state.settings[control.key] = [...gRows];
      saveState();
      renderCode(); renderDecisionGuide(); renderNav();
      updateGroupActionState();
    };

    const table = document.createElement("table");
    table.className = "tag-table group-table";

    const renderGTableRows = () => {
      table.innerHTML = "";
      const head = document.createElement("tr");
      ["Group Prefix", "Action", ""].forEach((h) => {
        const th = document.createElement("th");
        th.textContent = h;
        head.appendChild(th);
      });
      table.appendChild(head);

      gRows.forEach((row, i) => {
        const tr = document.createElement("tr");

        // Prefix cell
        const prefixTd = document.createElement("td");
        const prefixInp = document.createElement("input");
        prefixInp.type = "text";
        prefixInp.value = row.prefix;
        prefixInp.disabled = locked;
        prefixInp.placeholder = "(6,)";
        prefixInp.addEventListener("change", () => { gRows[i].prefix = prefixInp.value; saveGRows(); });
        prefixTd.appendChild(prefixInp);
        tr.appendChild(prefixTd);

        // Action cell
        const actionTd = document.createElement("td");
        const actionSel = document.createElement("select");
        actionSel.disabled = locked;
        GROUP_ACTIONS.forEach((a) => {
          const opt = document.createElement("option");
          opt.value = a;
          opt.textContent = a;
          if (a === row.action) opt.selected = true;
          actionSel.appendChild(opt);
        });
        actionSel.addEventListener("change", () => { gRows[i].action = actionSel.value; saveGRows(); });
        actionTd.appendChild(actionSel);
        tr.appendChild(actionTd);

        // Delete cell
        const delTd = document.createElement("td");
        const delBtn = document.createElement("button");
        delBtn.type = "button";
        delBtn.className = "tag-row-del";
        delBtn.textContent = "✕";
        delBtn.disabled = locked;
        delBtn.addEventListener("click", () => { gRows.splice(i, 1); saveGRows(); renderGTableRows(); });
        delTd.appendChild(delBtn);
        tr.appendChild(delTd);

        table.appendChild(tr);
      });
    };

    renderGTableRows();

    const addBtn = document.createElement("button");
    addBtn.type = "button";
    addBtn.className = "tag-row-add";
    addBtn.textContent = "+ Add group";
    addBtn.disabled = locked;
    addBtn.addEventListener("click", () => {
      gRows.push({ prefix: "", action: "remove" });
      saveGRows(); renderGTableRows();
    });

    const uploadInput = document.createElement("input");
    uploadInput.type = "file";
    uploadInput.accept = ".csv,text/csv";
    uploadInput.hidden = true;

    const uploadBtn = document.createElement("button");
    uploadBtn.type = "button";
    uploadBtn.className = "tag-row-upload";
    uploadBtn.textContent = "Upload CSV";
    uploadBtn.disabled = locked;
    uploadBtn.addEventListener("click", () => uploadInput.click());
    uploadInput.addEventListener("change", async () => {
      const file = uploadInput.files && uploadInput.files[0];
      uploadInput.value = "";
      if (!file) return;
      const uploadedRows = groupRowsFromCsv(await file.text());
      if (!uploadedRows.length) {
        state.message = "No group rows found in the uploaded CSV.";
        saveState();
        renderNav();
        return;
      }
      gRows.length = 0;
      uploadedRows.forEach((r) => gRows.push(r));
      saveGRows();
      renderGTableRows();
    });

    const clearBtn = document.createElement("button");
    clearBtn.type = "button";
    clearBtn.className = "tag-row-clear";
    clearBtn.textContent = "Clear";
    clearBtn.disabled = locked || gRows.length === 0;
    clearBtn.addEventListener("click", () => {
      gRows.length = 0;
      state.settings[control.key] = [];
      delete state.decided[decisionKey(control.key)];
      invalidateFrom(state.stepIndex);
      state.message = "Group rules cleared.";
      saveState();
      render();
    });
    updateGroupActionState = () => {
      clearBtn.disabled = locked || gRows.length === 0;
    };

    const tableActions = document.createElement("div");
    tableActions.className = "tag-table-actions";
    tableActions.appendChild(addBtn);
    tableActions.appendChild(uploadBtn);
    tableActions.appendChild(clearBtn);
    tableActions.appendChild(uploadInput);
    updateGroupActionState();

    inputArea.appendChild(tableActions);
    inputArea.appendChild(table);
  }

  // ── action-button control ─────────────────────────────────────────────────
  if (control.type === "action-button") {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "action-btn";
    const alreadyDone = !!state.settings[control.key];
    btn.textContent = alreadyDone ? (control.savedLabel || "Done ✓") : (control.actionLabel || "Run");
    btn.disabled = locked;
    btn.addEventListener("click", () => {
      const s = state.settings;
      if (control.action === "downloadStrategyCsv") {
        const artifact = currentStrategyArtifact(s);
        downloadCsv(artifact.tagCsv, `${strategySlug(artifact.name)}.csv`);
        if (artifact.groupPath) downloadCsv(artifact.groupCsv, `${strategySlug(artifact.groupName)}.csv`);
        state.settings[control.key] = true;
        saveState();
        render();
      }
    });
    inputArea.appendChild(btn);
  }


  // Accept button
  const accept = document.createElement("button");
  accept.className = `accept-decision ${accepted ? "accepted" : ""}`;
  accept.type = "button";
  accept.textContent = accepted ? "Accepted" : "Accept";
  accept.disabled = locked || (control.required && !isEffectivelyFilled(control));
  accept.addEventListener("click", () => { recordDecision(control.key); render(); });
  if (!state.godMode) inputArea.appendChild(accept);

  wrapper.appendChild(meta);
  wrapper.appendChild(inputArea);
  wrapper.appendChild(impact);
  return wrapper;
}

// ─── Code generation ─────────────────────────────────────────────────────────
function stepCode() {
  if (!isStageConfirmed()) return "# Confirm this stage to view its generated code.";
  return currentStep().code(state.settings);
}

function fullCode() {
  if (!allStagesConfirmed()) return "# Confirm all stages to generate the full script.";
  const wf = currentWorkflow();
  if (wf.fullCode) return wf.fullCode(state.settings);
  return wf.steps.map((st) => st.code(state.settings)).filter((s) => s.trim()).join("\n\n");
}

function renderCode() {
  const el    = document.querySelector("#codeOutput");
  const raw   = state.codeMode === "step" ? stepCode() : fullCode();
  el.innerHTML = highlight(raw);

  document.querySelector("#stepCodeButton").classList.toggle("active", state.codeMode === "step");
  document.querySelector("#fullCodeButton").classList.toggle("active", state.codeMode === "full");
  document.querySelector("#stepCodeButton").disabled = !isStageConfirmed();
  document.querySelector("#fullCodeButton").disabled = !allStagesConfirmed();
  document.querySelector("#codeGateStatus").textContent = allStagesConfirmed()
    ? "Full script ready"
    : isStageConfirmed()
      ? "Current step code available"
      : "Locked until stage is confirmed";
  document.querySelector(".code-drawer").classList.toggle("collapsed", !state.codeOpen);
}

function renderNav() {
  const prev    = document.querySelector("#prevStep");
  const next    = document.querySelector("#nextStep");
  const confirm = document.querySelector("#confirmStep");
  const reopen  = document.querySelector("#reopenStep");
  const status  = document.querySelector("#decisionStatus");

  const isOverview = currentStep().kind === "Overview";
  const missing    = missingRequiredControls(currentStep());
  const confirmed  = isStageConfirmed();
  const steps       = currentWorkflow().steps;
  const lastVisibleIdx = steps.reduce((acc, _, i) => isStepVisible(i) ? i : acc, 0);
  const last       = state.stepIndex === lastVisibleIdx;

  prev.disabled    = state.stepIndex === 0;
  next.disabled    = !confirmed || last;
  const understood = isStageUnderstood();

  // Overview steps: hide confirm/reopen chrome entirely — they're auto-confirmed
  confirm.style.display = isOverview ? "none" : "";
  reopen.style.display  = isOverview ? "none" : confirmed ? "" : "none";
  if (!isOverview) {
    confirm.disabled = missing.length > 0 || !understood || confirmed;
    confirm.textContent = confirmed ? "Confirmed" : "Confirm Stage";
  }

  if (isOverview) {
    status.textContent = "";
  } else if (state.message) {
    status.textContent = state.message;
  } else if (!understood) {
    status.textContent = "Complete the stage review before continuing.";
  } else if (missing.length) {
    status.textContent = `Accept: ${missing.map((c) => c.label).join(", ")}.`;
  } else if (confirmed) {
    status.textContent = "Stage confirmed. Click Re-open to edit decisions.";
  } else {
    status.textContent = "Review complete. Confirm this stage to continue.";
  }

  // Completion banner
  const banner = document.querySelector("#completionBanner");
  if (allStagesConfirmed()) {
    banner.hidden = false;
    document.querySelector("#bannerWorkflow").textContent = currentWorkflow().name;
    if (!state.codeOpen) {
      state.codeOpen = true;
    }
  } else {
    banner.hidden = true;
  }
}

function renderWorkflowDesc() {
  const el = document.querySelector("#workflowDescText");
  if (el) el.textContent = currentWorkflow().description || "";
}


// ─── God mode ────────────────────────────────────────────────────────────────
// Ctrl+Shift+G toggles god mode. When on, every render auto-records all filled
// controls, auto-records decisions, and auto-confirms the stage.
// Forces the user to make selections first (controls without a value are left
// unrecorded so the Confirm gate still blocks).
// Auto-confirm overview steps — they're informational, no user action needed
function autoConfirmOverviewSteps() {
  currentWorkflow().steps.forEach((step, i) => {
    if (step.kind === "Overview" && isStepVisible(i) && !state.completed[stepKey(i)]) {
      state.completed[stepKey(i)] = true;
    }
  });
}

function applyGodMode() {
  if (!state.godMode) return;
  const step = currentStep();
  const vis  = interactiveControls(step);

  // Auto-record all interactive controls that already have a value
  vis.forEach((c) => {
    if (isEffectivelyFilled(c)) recordDecision(c.key);
  });


  // Auto-check every mustUnderstand item
  stageUnderstandingItems(step).forEach((_, j) => {
    state.understood[understandingKey(state.stepIndex, j)] = true;
  });

  // Auto-confirm if all required decisions are now met
  if (!isStageConfirmed() && missingRequiredControls(step).length === 0) {
    state.completed[stepKey()] = true;
  }
}

function render() {
  // Clamp stepIndex in case steps were removed since state was last saved
  const _maxStep = currentWorkflow().steps.length - 1;
  if (state.stepIndex > _maxStep) state.stepIndex = Math.max(0, _maxStep);

  autoConfirmOverviewSteps();
  applyGodMode();
  // If current step is now hidden (e.g. toggle turned off), back up to nearest visible step
  if (!isStepVisible(state.stepIndex)) {
    for (let j = state.stepIndex - 1; j >= 0; j--) {
      if (isStepVisible(j)) { state.stepIndex = j; break; }
    }
  }
  renderWorkflowDesc();
  renderSteps();
  renderDetail();
  renderCode();
  renderNav();
  saveState();
}

// ─── Event listeners ──────────────────────────────────────────────────────────
document.querySelector("#workflowProfile").addEventListener("change", (e) => {
  state.workflow = e.target.value;
  state.stepIndex = 0;
  state.message = "";
  state.clearChoiceSummary = true;
  initSettings();
  render();
  state.clearChoiceSummary = false;
});

document.querySelector("#prevStep").addEventListener("click", () => {
  let prev = state.stepIndex - 1;
  while (prev > 0 && !isStepVisible(prev)) prev--;
  state.stepIndex = Math.max(0, prev);
  state.message = "";
  render();
});

document.querySelector("#nextStep").addEventListener("click", () => {
  if (!isStageConfirmed()) { state.message = "Confirm this stage before continuing."; renderNav(); return; }
  const total = currentWorkflow().steps.length;
  let next = state.stepIndex + 1;
  while (next < total && !isStepVisible(next)) next++;
  state.stepIndex = Math.min(total - 1, next);
  state.message = "";
  render();
});

document.querySelector("#confirmStep").addEventListener("click", () => {
  if (!isStageUnderstood()) { state.message = "Complete the stage review first."; renderNav(); return; }
  const missing = missingRequiredControls(currentStep());
  if (missing.length) { state.message = `Accept: ${missing.map((c) => c.label).join(", ")}.`; renderNav(); return; }
  state.completed[stepKey()] = true;
  state.message = "Stage confirmed. Next stage unlocked.";
  render();
});

document.querySelector("#reopenStep").addEventListener("click", () => {
  delete state.completed[stepKey()];
  invalidateFrom(state.stepIndex);
  state.message = "Stage re-opened. Update decisions and confirm again.";
  render();
});

document.querySelector("#stepCodeButton").addEventListener("click", () => { state.codeMode = "step"; renderCode(); });
document.querySelector("#fullCodeButton").addEventListener("click", () => { state.codeMode = "full"; renderCode(); });
document.querySelector("#toggleCode").addEventListener("click", () => { state.codeOpen = !state.codeOpen; renderCode(); saveState(); });

document.querySelector("#copyCode").addEventListener("click", async () => {
  const raw = document.querySelector("#codeOutput").textContent;
  const st  = document.querySelector("#copyStatus");
  try {
    await navigator.clipboard.writeText(raw);
    st.textContent = "Code copied to clipboard.";
  } catch {
    st.textContent = "Copy failed — select the code block and copy manually.";
  }
});


// ─── God mode keyboard shortcut (Ctrl+Shift+G) ───────────────────────────────
document.addEventListener("keydown", (e) => {
  if (e.ctrlKey && e.shiftKey && e.key === "G") {
    e.preventDefault();
    state.godMode = !state.godMode;
    document.querySelector("#godModeBadge").hidden = !state.godMode;
    render();
  }
});

document.querySelector("#resetAll").addEventListener("click", () => {
  if (!confirm("Reset all stages and settings for this workflow?")) return;
  const wf = state.workflow;
  // Clear completed, understood, decided keys for this workflow
  for (const k of Object.keys(state.understood)) { if (k.startsWith(wf + ":")) delete state.understood[k]; }
  for (const k of Object.keys(state.completed))  { if (k.startsWith(wf + ":")) delete state.completed[k]; }
  for (const k of Object.keys(state.decided))    { if (k.startsWith(wf + ":")) delete state.decided[k]; }
  resetWorkflowMemory(wf);
  initSettings();
  state.stepIndex = 0;
  state.message = "";
  state.clearChoiceSummary = true;
  render();
  state.clearChoiceSummary = false;
});

// ─── Init ─────────────────────────────────────────────────────────────────────
loadState();
initSettings();

// Sync workflow selector to restored state
document.querySelector("#workflowProfile").value = state.workflow;

render();
