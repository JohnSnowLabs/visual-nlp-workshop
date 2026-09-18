// ─── Fixed runtime constants ───────────────────────────────────────────────
const FIXED = {
  dicomPath: "/path/to/dicom/files",
  outputDir: "/tmp/dicom_deid",
  imageCol: "image",
  textRegionsCol: "text_regions",
  pixelOutputCol: "dicom_pixel_cleaned",
  metadataOutputCol: "dicom_metadata_cleaned",
  strategyFilePath: "/tmp/dicom_metadata_strategy.csv",
};

// ─── Shared stage factories ─────────────────────────────────────────────────
function pyBool(v) { return v ? "True" : "False"; }

function strategyName(value) {
  return String(value || "my_strategy").trim() || "my_strategy";
}

function strategySlug(value) {
  return strategyName(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "") || "my_strategy";
}

function strategyPathFromName(name, suffix = "") {
  return `/tmp/${strategySlug(name)}${suffix}.csv`;
}

function groupStrategyName(settings = state.settings) {
  return strategyName(settings.sf_groupName || `${strategyName(settings.sf_name)}_group`);
}

function tagStrategyCsv(rows = []) {
  return [
    "Tags,VR,Name,Status,Action,Option",
    ...rows.map((r) => `"${r.tag}",${r.vr},${r.name || ""},,${r.action},${r.option || ""}`),
  ].join("\n");
}

function groupStrategyCsv(rows = []) {
  return [
    "Tags,VR,Name,Status,Action,Option",
    ...rows.map((r) => `"${r.prefix}",GROUP,None,,${r.action},`),
  ].join("\n");
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let cell = "";
  let quoted = false;

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    const next = text[i + 1];
    if (quoted) {
      if (ch === '"' && next === '"') {
        cell += '"';
        i++;
      } else if (ch === '"') {
        quoted = false;
      } else {
        cell += ch;
      }
    } else if (ch === '"') {
      quoted = true;
    } else if (ch === ",") {
      row.push(cell.trim());
      cell = "";
    } else if (ch === "\n") {
      row.push(cell.trim());
      if (row.some(Boolean)) rows.push(row);
      row = [];
      cell = "";
    } else if (ch !== "\r") {
      cell += ch;
    }
  }

  row.push(cell.trim());
  if (row.some(Boolean)) rows.push(row);
  return rows;
}

function headerIndex(headers, names) {
  const normalized = headers.map((h) => String(h).toLowerCase().replace(/[^a-z0-9]/g, ""));
  return names.map((n) => n.toLowerCase().replace(/[^a-z0-9]/g, ""))
    .map((n) => normalized.indexOf(n))
    .find((i) => i >= 0) ?? -1;
}

function csvHasHeader(row) {
  return row.some((cell) => /^(tags?|vr|name|status|action|option|group\s*prefix|prefix)$/i.test(String(cell).trim()));
}

function normalizeDicomCsvRow(row) {
  if (
    row.length > 1
    && /^\([0-9a-fA-F]+$/.test(row[0])
    && /^[0-9a-fA-F]*\)$/.test(row[1])
  ) {
    return [`${row[0]},${row[1]}`, ...row.slice(2)];
  }
  return row;
}

function tagRowsFromCsv(text) {
  const rows = parseCsv(text).map(normalizeDicomCsvRow);
  if (!rows.length) return [];
  const headers = csvHasHeader(rows[0]) ? rows.shift() : [];
  const idx = headers.length ? {
    tag: headerIndex(headers, ["tags", "tag"]),
    vr: headerIndex(headers, ["vr"]),
    name: headerIndex(headers, ["name"]),
    action: headerIndex(headers, ["action"]),
    option: headerIndex(headers, ["option"]),
  } : { tag: 0, vr: 1, name: 2, action: 4, option: 5 };

  return rows.map((r) => ({
    tag: r[idx.tag] || "",
    vr: (r[idx.vr] || "LO").toUpperCase(),
    name: r[idx.name] || "",
    action: r[idx.action] || "remove",
    option: r[idx.option] || "",
  })).filter((r) => r.tag);
}

function groupRowsFromCsv(text) {
  const rows = parseCsv(text).map(normalizeDicomCsvRow);
  if (!rows.length) return [];
  const headers = csvHasHeader(rows[0]) ? rows.shift() : [];
  const idx = headers.length ? {
    prefix: headerIndex(headers, ["tags", "tag", "groupPrefix", "prefix"]),
    action: headerIndex(headers, ["action"]),
  } : { prefix: 0, action: 4 };

  return rows.map((r) => ({
    prefix: r[idx.prefix] || "",
    action: r[idx.action] || "remove",
  })).filter((r) => r.prefix);
}

function currentStrategyArtifact(settings = state.settings) {
  const name = strategyName(settings.sf_name);
  const groupName = settings.sf_includeGroup ? groupStrategyName(settings) : null;
  return {
    name,
    path: strategyPathFromName(name),
    groupName,
    groupPath: groupName ? strategyPathFromName(groupName) : null,
    delivery: settings.sf_delivery || "render",
    tagCsv: tagStrategyCsv(settings.sf_tagRows || []),
    groupCsv: settings.sf_includeGroup ? groupStrategyCsv(settings.sf_groupRows || []) : "",
  };
}

function savedStrategy(name) {
  return name && state.strategyFiles ? state.strategyFiles[name] : null;
}

function savedStrategyOptions() {
  const names = Object.keys(state.strategyFiles || {});
  if (!names.length) {
    return [{ label: "No named strategies yet", value: "", summary: "Build one in Strategy File Builder first." }];
  }
  return [
    { label: "Choose a named strategy", value: "", summary: "Select a Strategy File Builder output." },
    ...names.map((name) => {
    const file = state.strategyFiles[name] || {};
    return {
      label: name,
      value: name,
      summary: file.delivery === "download"
        ? "Generated code will reference the downloaded CSV."
        : "CSV will be rendered inline in generated code.",
    };
  })];
}

function namedStrategyCode(name, opts = {}) {
  const file = savedStrategy(name);
  if (!file) {
    return `# Pick a named strategy from Strategy File Builder before generating this workflow.
strategy_file_path = "${FIXED.strategyFilePath}"`;
  }

  const lines = [];
  if (file.delivery === "download") {
    lines.push(`# Named strategy "${file.name || name}" was downloaded from Strategy File Builder.`);
    lines.push(`# Place ${file.path.split("/").pop()} where your notebook can read it, or update this derived path.`);
    lines.push(`strategy_file_path = "${file.path}"`);
  } else {
    lines.push(`# Named strategy "${file.name || name}" rendered from Strategy File Builder.
from textwrap import dedent

csv_strategy_data = dedent("""\\
${file.tagCsv}
""")

strategy_file_path = "${file.path}"
with open(strategy_file_path, "w", encoding="utf-8", newline="") as file:
    file.write(csv_strategy_data)

print(f"Strategy file saved to: {strategy_file_path}")`);
  }

  if (opts.includeGroup && file.groupPath) {
    if (file.delivery === "download") {
      lines.push(`# Named group strategy "${file.groupName || `${file.name || name}_group`}" was downloaded from Strategy File Builder.`);
      lines.push(`group_strategy_file_path = "${file.groupPath}"`);
    } else {
      lines.push(`
# Named group strategy "${file.groupName || `${file.name || name}_group`}" rendered from Strategy File Builder.
csv_group_strategy_data = dedent("""\\
${file.groupCsv}
""")

group_strategy_file_path = "${file.groupPath}"
with open(group_strategy_file_path, "w", encoding="utf-8", newline="") as file:
    file.write(csv_group_strategy_data)

print(f"Group strategy file saved to: {group_strategy_file_path}")`);
    }
  }

  return lines.join("\n");
}

function loadDicomStage() {
  return {
    title: "Load DICOM",
    kind: "Required",
    description: "Read DICOM files as binary records with `path` and `content` columns.",
    purpose: "This step is informational. Paths and input columns are environment details, not de-identification decisions.",
    output: "`dicom_df` with `path` and `content`.",
    notes: "The generated code uses a placeholder path. Replace it in your notebook.",
    mustUnderstand: [
      "The browser UI does not upload, inspect, or process DICOM files.",
      "`content` carries raw DICOM bytes used by image and metadata stages.",
      "`path` is preserved for writing cleaned DICOM bytes back to disk.",
    ],
    controls: [],
    decisionSummary: () => ["No result-changing configuration in this stage."],
    code: () => `dicom_path = "${FIXED.dicomPath}"
dicom_df = spark.read.format("binaryFile").load(dicom_path)`,
  };
}

function metadataDeidStage(inputColKey, inputColValue) {

  const INLINE_TAGS = [
    {
      tag: "(0010, 0010)", vr: "PN", name: "Patient Name", key: "patientName",
      defaultAction: "replaceWithLiteral",
      options: [
        { label: "Replace with literal (<REMOVED>)", value: "replaceWithLiteral", option: "<REMOVED>", summary: "Replaces name with <REMOVED>." },
        { label: "Replace with random name", value: "replaceWithRandomName", option: "", summary: "Replaces with a generated random name." },
        { label: "Remove value", value: "remove", option: "", summary: "Empties the field." },
        { label: "Delete tag", value: "delete", option: "", summary: "Removes the tag entirely." },
      ],
    },
    {
      tag: "(0010, 0020)", vr: "LO", name: "Patient ID", key: "patientId",
      defaultAction: "hashId",
      options: [
        { label: "Hash (deterministic)", value: "hashId", option: "", summary: "Consistent hash of the ID." },
        { label: "Remove value", value: "remove", option: "", summary: "Empties the field." },
        { label: "Replace with literal (<REMOVED>)", value: "replaceWithLiteral", option: "<REMOVED>", summary: "Replaces with <REMOVED>." },
        { label: "Delete tag", value: "delete", option: "", summary: "Removes the tag entirely." },
      ],
    },
    {
      tag: "(0010, 0030)", vr: "DA", name: "Patient Birth Date", key: "patientBirthDate",
      defaultAction: "remove",
      options: [
        { label: "Remove value", value: "remove", option: "", summary: "Empties the birth date." },
        { label: "Shift by random days", value: "shiftDateByRandomNbOfDays", option: "", summary: "Randomly shifts the date." },
        { label: "Delete tag", value: "delete", option: "", summary: "Removes the tag entirely." },
      ],
    },
    {
      tag: "(0008, 0080)", vr: "LO", name: "Institution Name", key: "institutionName",
      defaultAction: "remove",
      options: [
        { label: "Remove value", value: "remove", option: "", summary: "Empties the institution name." },
        { label: "Replace with literal (<REMOVED>)", value: "replaceWithLiteral", option: "<REMOVED>", summary: "Replaces with <REMOVED>." },
        { label: "Delete tag", value: "delete", option: "", summary: "Removes the tag entirely." },
      ],
    },
    {
      tag: "(0008, 0090)", vr: "PN", name: "Referring Physician Name", key: "referringPhysician",
      defaultAction: "replaceWithRandomName",
      options: [
        { label: "Replace with random name", value: "replaceWithRandomName", option: "", summary: "Replaces with a generated physician name." },
        { label: "Replace with literal (<REMOVED>)", value: "replaceWithLiteral", option: "<REMOVED>", summary: "Replaces with <REMOVED>." },
        { label: "Remove value", value: "remove", option: "", summary: "Empties the field." },
        { label: "Delete tag", value: "delete", option: "", summary: "Removes the tag entirely." },
      ],
    },
    {
      tag: "(0008, 1030)", vr: "LO", name: "Study Description", key: "studyDescription",
      defaultAction: "remove",
      options: [
        { label: "Remove value", value: "remove", option: "", summary: "Empties the description." },
        { label: "Hash", value: "hashId", option: "", summary: "Hashes the description value." },
        { label: "Replace with literal (<REMOVED>)", value: "replaceWithLiteral", option: "<REMOVED>", summary: "Replaces with <REMOVED>." },
        { label: "Delete tag", value: "delete", option: "", summary: "Removes the tag entirely." },
      ],
    },
    {
      tag: "(0008, 0050)", vr: "SH", name: "Accession Number", key: "accessionNumber",
      defaultAction: "hashId",
      options: [
        { label: "Hash (deterministic)", value: "hashId", option: "", summary: "Consistent hash of the accession number." },
        { label: "Remove value", value: "remove", option: "", summary: "Empties the field." },
        { label: "Delete tag", value: "delete", option: "", summary: "Removes the tag entirely." },
      ],
    },
    {
      tag: "(0008, 0020)", vr: "DA", name: "Study Date", key: "studyDate",
      defaultAction: "shiftDateByRandomNbOfDays",
      options: [
        { label: "Shift by random days", value: "shiftDateByRandomNbOfDays", option: "", summary: "Randomly shifts the date." },
        { label: "Remove value", value: "remove", option: "", summary: "Empties the date." },
        { label: "Delete tag", value: "delete", option: "", summary: "Removes the tag entirely." },
      ],
    },
    {
      tag: "(0020, 000D)", vr: "UI", name: "Study Instance UID", key: "studyInstanceUID",
      defaultAction: "hashId",
      options: [
        { label: "Hash (deterministic)", value: "hashId", option: "", summary: "Consistent hash of the UID." },
        { label: "Remove value", value: "remove", option: "", summary: "Empties the field." },
        { label: "Delete tag", value: "delete", option: "", summary: "Removes the tag entirely." },
      ],
    },
  ];

  // Pre-populate default rows from INLINE_TAGS for the tag-table control
  const defaultTagRows = INLINE_TAGS.map((t) => ({
    tag: t.tag, vr: t.vr, name: t.name, action: t.defaultAction,
    option: (t.options.find((o) => o.value === t.defaultAction) || {}).option || "",
  }));

  return {
    title: "Metadata De-identification",
    kind: "Optional",
    description: "Clean structured DICOM metadata tags after pixel redaction.",
    purpose: "Pixel redaction only cleans burned-in text. Metadata PHI in DICOM tags requires a separate strategy-file-driven step.",
    output: "`dicom_metadata_cleaned` if included; otherwise final output stays `dicom_pixel_cleaned`.",
    notes: "Use a named strategy from Strategy File Builder, or build a starter strategy CSV inline.",
    mustUnderstand: [
      "Pixel redaction does not touch structured metadata tags.",
      "A strategy CSV mapping tag actions (hash, remove, replace, shift dates) must exist before the pipeline runs.",
      "Omitting this step leaves metadata unchanged.",
    ],
    controls: [

      {
        type: "toggle",
        key: inputColKey + "_removePrivateTags",
        label: "Remove private tags",
        value: false,
        required: true,
        
        help: "Private DICOM tags hold vendor-specific data that may contain PHI.",
        impact: (v) => v ? "Private tags will be removed." : "Private tags are preserved unless the strategy file covers them.",
      },


      {
        type: "tag-table",
        key: inputColKey + "_strategyRows",
        label: "Strategy tags",
        value: defaultTagRows,
        required: true,
        when: (s) => s[inputColKey + "_strategySource"] === "inline",
        help: "Add DICOM tags and choose a de-identification action for each. Common tags are pre-populated — add or remove rows as needed.",
      },
      {
        type: "toggle",
        key: inputColKey + "_useGroupStrategy",
        label: "Include group de-identification",
        value: false,
        required: true,
        help: "Apply a group-level strategy CSV in addition to the tag-level strategy.",
        impact: (v) => v ? "Group de-identification will be applied." : "No group de-identification.",
      },

    ],
    decisionSummary: (s) => {
      const stratName = inputColKey === "bl" ? s.bl_sharedStrategyName : s[inputColKey + "_savedStrategyName"];
      const groupEnabled = Boolean(s[inputColKey + "_useGroupStrategy"]);
      const lines = [
        "Metadata de-identification included after pixel redaction.",
        s[inputColKey + "_removePrivateTags"] ? "Private tags will be removed." : "Private tags not automatically removed.",
        stratName ? `Strategy: "${stratName}".` : "No strategy selected.",
      ];
      if (groupEnabled) lines.push("Group de-identification enabled.");
      return lines;
    },
    code: (s) => {
      const stratName = inputColKey === "bl" ? s.bl_sharedStrategyName : s[inputColKey + "_savedStrategyName"];
      const selectedStrategy = savedStrategy(stratName);
      const groupEnabled = Boolean(s[inputColKey + "_useGroupStrategy"]);

      const groupStratPath = FIXED.strategyFilePath.replace(".csv", "_group.csv");
      const groupVarLine = groupEnabled ? `group_strategy_file = "${groupStratPath}"\n\n` : "";
      const groupLine    = groupEnabled ? ` \\\n    .setGroupStrategyFile(group_strategy_file)` : "";

      const strategyBlock = s[inputColKey + "_extractTagForNER"] !== true
        ? `strategy_file_path = "${FIXED.strategyFilePath}"`
        : "";

      const prefix = strategyBlock ? `${strategyBlock}\n\n` : "";

      return `${prefix}${groupVarLine}dicom_deidentifier = DicomMetadataDeidentifier() \\
    .setInputCols(["${inputColValue}"]) \\
    .setOutputCol("${FIXED.metadataOutputCol}") \\
    .setKeepInput(False) \\
    .setStrategyFile(strategy_file_path) \\
    .setRemovePrivateTags(${pyBool(s[inputColKey + "_removePrivateTags"])})${groupLine}`;
    },
  };
}

function validateSaveStage(finalColFn) {
  return {
    title: "Validate and Save",
    kind: "Required",
    description: "Preview the final DICOM bytes and write cleaned files to disk.",
    purpose: "Verification before saving at scale ensures the pipeline produced expected results.",
    output: "Saved de-identified DICOM files.",
    notes: "Preview size and output path are placeholder values. Adjust in your notebook.",
    mustUnderstand: [
      "Always inspect at least one cleaned DICOM before running at scale.",
      "The final column depends on whether metadata de-identification was included.",
      "Cleaned files are written to the configured output directory.",
    ],
    decisionSummary: (s) => [`Final DICOM column: \`${finalColFn(s)}\`.`],
    controls: [
      {
        type: "choice", key: "includeSaveHelper", label: "Include Dicom Disk Saver Function",
        value: "yes",
        options: [
          { label: "Yes", value: "yes", summary: "Includes the save_dicom_to_disk() helper function and its call site." },
          { label: "No",  value: "no",  summary: "Omits the save helper — only display_dicom is shown." },
        ],
        help: "Choose Yes to include the save_dicom_to_disk() function definition and its call in the code block.",
      },
    ],
    code: (s) => {
      const col = finalColFn(s);
      const includeSave = s.includeSaveHelper !== "no";
      const saveFn = includeSave ? `def save_dicom_to_disk(dataframe, dicom_col="dicom_metadata_cleaned", output_dir="/tmp/dicom_deid"):
    from pathlib import Path

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    saved_paths = []

    for row in dataframe.select("path", dicom_col).toLocalIterator():
        base_file_name = Path(row["path"]).name
        target_path = output_path / base_file_name
        dicom_bytes = row[dicom_col]
        if isinstance(dicom_bytes, bytearray):
            dicom_bytes = bytes(dicom_bytes)
        with open(target_path, "wb") as f:
            f.write(dicom_bytes)
        saved_paths.append(str(target_path))

    return saved_paths\n\n` : "";
      const saveCall = includeSave ? `\nsaved_paths = save_dicom_to_disk(result, dicom_col="${col}", output_dir="${FIXED.outputDir}")\nsaved_paths[:5]` : "";
      return `${saveFn}display_dicom(df=result, fields="${col}", limit=1, width=300)${saveCall}`;
    },
  };
}

function downloadCsv(content, filename) {
  const blob = new Blob([content], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

