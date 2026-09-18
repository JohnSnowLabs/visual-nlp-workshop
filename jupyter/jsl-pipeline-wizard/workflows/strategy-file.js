const workflowStrategyFile = {
    name: "DICOM Strategy File Builder",
    description: "Build and name a DICOM metadata de-identification strategy CSV.",
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
          "Builds a DICOM de-identification strategy CSV mapping tags to actions.",
          "Optionally adds a group-level strategy for bulk tag group removal.",
          ],
          outputs: [
          "A named strategy CSV referenced by other de-id workflows.",
          "Optionally a group strategy CSV for overlay/bulk group removal.",
          ],
          prerequisites: [
          "Know which PHI tags your DICOM files contain.",
          "Decide how each tag should be handled (hash, remove, replace, shift, etc.).",
          ],
          whenToUse: [
          "Before running any metadata de-identification workflow.",
          "When you need a reusable, version-controlled de-id policy.",
          ],
        },
        },
      ],
      decisionSummary: () => [],
      code: () => "",
    },


      // Step 0 — Setup
      {
        title: "Setup",
        kind: "Required",
        description: "Name your strategy & group strategy file.",
        output: "A named strategy file entry stored in this session.",
        mustUnderstand: [
          "Strategy file can used for de-id DICOM header. All actions are supported in the strategy file.",
          "Group strategy files handle bulk DICOM group removal (all overlay data). Only remove/delete actions are supported.",
        ],
        controls: [
          {
            type: "text",
            key: "sf_name",
            label: "Strategy file name",
            value: "my_strategy",
            required: true,
            help: "A short label for this file (e.g. 'study_deid'). Used to reference it in other workflows.",
            impact: (v) => v ? `Strategy named: "${v}".` : "No name set.",
          },
          {
            type: "toggle",
            key: "sf_includeGroup",
            label: "Include group strategy",
            value: false,
            required: false,
            help: "Also build a group-level strategy CSV for bulk DICOM group handling (e.g. remove overlay data).",
            impact: (v) => v ? "Group strategy step will be included." : "Tag strategy only.",
          },
          {
            type: "text",
            key: "sf_groupName",
            label: "Group strategy file name",
            value: "",
            required: true,
            when: (s) => s.sf_includeGroup === true,
            placeholder: (s) => `${strategyName(s.sf_name)}_group`,
            help: "Optional name for the group strategy. Leave blank to use the strategy name with a group suffix.",
            impact: (v, s) => `Group strategy named: "${groupStrategyName({ ...s, sf_groupName: v })}".`,
          },
        ],
        decisionSummary: (s) => {
          const artifact = currentStrategyArtifact(s);
          const lines = [`Strategy name: "${artifact.name}".`, "Notebook filenames will be derived from this name."];
          if (s.sf_includeGroup) lines.push(`Group strategy name: "${artifact.groupName}".`);
          return lines;
        },
        code: (s) => {
          const artifact = currentStrategyArtifact(s);
          return `strategy_name = "${artifact.name}"
strategy_file_path = "${artifact.path}"${artifact.groupName ? `\ngroup_strategy_name = "${artifact.groupName}"\ngroup_strategy_file_path = "${artifact.groupPath}"` : ""}`;
        },
      },


      // Step 1 — Action Reference
      {
        title: "Action Reference",
        kind: "Reference",
        description: "All actions supported.",
        output: "No code output — reference only.",
        controls: [
          {
            type: "reference-table",
            key: "sf_actionRef",
            label: "Available actions",
            required: false,
            data: [
              { category: true, label: "Removal" },
              { action: "delete",  vrs: "All",     option: "—",                   effect: "Physically removes the tag from the DICOM file." },
              { action: "remove",  vrs: "All",     option: "—",                   effect: "Keeps the tag but zeroes its value (empty string, 0, or empty bytes depending on VR)." },

              { category: true, label: "Replacement" },
              { action: "replaceWithLiteral",                                         vrs: "All",       option: "The literal value to write",   effect: "Sets the tag to a fixed value. Numeric VRs validate the value is in range." },
              { action: "replaceWithRandomName",                                       vrs: "PN, LO",    option: "coherent (stable pseudonym)",  effect: "Replaces with a random First Last name. coherent mode seeds by original value so the same input always gets the same name." },

              { category: true, label: "Hashing / Pseudonymization" },
              { action: "hashId",        vrs: "UI, LO, SH", option: "—", effect: "Deterministic DICOM UID (2.25.<hash>). Same input always produces the same pseudonym — safe for cross-file linkage." },
              { action: "patientHashId", vrs: "LO",          option: "—", effect: "SHA1 digit-hash of the patient ID, preserving the original length." },

              { category: true, label: "Date & Time Shifting" },
              { action: "shiftDateByRandomNbOfDays",  vrs: "DA, DT", option: "Max days to shift (default 60)", effect: "Shifts the date by a random negative offset within the given range." },
              { action: "shiftDateByFixedNbOfDays",   vrs: "DA, DT", option: "Days (integer, negative = past)", effect: "Shifts the date by an exact fixed number of days." },
              { action: "shiftTimeByRandom",           vrs: "TM",     option: "fractional (add microseconds)",  effect: "Replaces with a random HH:MM:SS time." },
              { action: "shiftUnixTimeStampRandom",    vrs: "SL, FD", option: "—",                              effect: "Shifts a unix timestamp to a random past value (within 60 days)." },
              { category: true, label: "Age" },
              { action: "shiftAgeByRandom",   vrs: "AS", option: "—", effect: "Replaces with a random age string in DICOM AS format (e.g. 042Y, 003M)." },
              { action: "capAgeAt99IfOver90", vrs: "AS", option: "—", effect: "If the age unit is Y and the value exceeds 90, sets it to 099Y. Otherwise leaves the value unchanged." },

              { category: true, label: "NER-based (requires DicomToMetadata upstream)" },
              { action: "cleanTag", vrs: "PN, UI, SH, CS, LO, LT, ST, IS, TM, AE, US, AS, DS, OB, DT, OW, UT", option: "remove · deid · mask", effect: "Replaces PHI entity spans detected by the NLP pipeline. remove: strips placeholders entirely. deid: keeps <ENTITY> tags as-is. mask: replaces with asterisks (length = entity label)." },

              { category: true, label: "Tag Management" },
              { action: "ensureTagExists", vrs: "All", option: "—", effect: "No-op if the tag already has a value. Creates the tag with the VR default (empty string, 0, empty bytes, empty SQ) if the tag is missing or empty." },
            ],
          },
        ],
        decisionSummary: () => ["Action reference reviewed."],
        code: () => "",
      },

      // Step 2 — Per-Tag Rules
      {
        title: "Per-Tag Rules",
        kind: "Required",
        description: "Define an action for each DICOM PHI tag.",
        output: "A CSV with columns: Tags, VR, Name, Status, Action, Option.",
        mustUnderstand: [
          "Action must be compatible with the tag's VR (Value Representation).",
          "Presets like HIPPA Safe Harbour, DICOM PS3.15 Basic and DICOM Minimal can be used a baseline.",
          "Existing Strategy file can be uploaded from disk and edited/improved based on use-case."
        ],
        controls: [
          {
            type: "tag-table",
            key: "sf_tagRows",
            label: "Tag rules",
            value: null,
            required: true,
            help: "Add, edit, or remove rows. Each row becomes one line in the strategy CSV.",
          },
        ],
        decisionSummary: (s) => {
          const rows = s.sf_tagRows || [];
          return rows.length > 0
            ? [`${rows.length} tag rule${rows.length === 1 ? "" : "s"} defined.`]
            : ["No tag rules defined yet."];
        },
        code: (s) => {
          const artifact = currentStrategyArtifact(s);
          return `from textwrap import dedent

csv_strategy_data = dedent("""\
${artifact.tagCsv}
""")

strategy_file_path = "${artifact.path}"
with open(strategy_file_path, "w", encoding="utf-8", newline="") as f:
    f.write(csv_strategy_data)

print(f"Tag strategy saved to: {strategy_file_path}")`;
        },
      },

      // Step 3 — Group Rules (conditional)
      {
        title: "Group Strategy Rules",
        kind: "Optional",
        when: (s) => s.sf_includeGroup === true,
        description: "Define bulk actions for entire DICOM tag groups",
        output: "A group strategy CSV with columns: Tags, VR, Name, Status, Action, Option.",
        mustUnderstand: [
          "Group prefix format: '(6,)', '(60,)' means DICOM group 0x6000.",
          "Only 'remove' and 'delete' actions are allowed at group level.",
          "Omitting this step means no group-level removals are applied.",
        ],
        controls: [
          {
            type: "group-table",
            key: "sf_groupRows",
            label: "Group rules",
            value: null,
            required: true,
            when: (s) => s.sf_includeGroup === true,
            help: "Each row targets a DICOM tag group prefix. Use '(6,)' for group 0x6000, '(80,)' for group 0x5000.",
          },
        ],
        decisionSummary: (s) => {
          if (!s.sf_includeGroup) return ["Group strategy not included."];
          const rows = s.sf_groupRows || [];
          return rows.length > 0
            ? [`${rows.length} group rule${rows.length === 1 ? "" : "s"} defined.`]
            : ["No group rules defined yet."];
        },
        code: (s) => {
          if (!s.sf_includeGroup) return "# Group strategy not included.";
          const artifact = currentStrategyArtifact(s);
          return `from textwrap import dedent

csv_group_strategy_data = dedent("""\
${artifact.groupCsv}
""")

group_strategy_file_path = "${artifact.groupPath}"
with open(group_strategy_file_path, "w", encoding="utf-8", newline="") as f:
    f.write(csv_group_strategy_data)

print(f"Group strategy saved to: {group_strategy_file_path}")`;
        },
      },

      // Step 4 — Delivery
      {
        title: "Delivery",
        kind: "Required",
        description: "Choose how this named strategy should be used.",
        output: "Either rendered CSV-writing code or downloaded CSV file(s), plus a session registry entry for this strategy.",
        mustUnderstand: [
          "Render in code block emits CSV content directly into the generated notebook code.",
          "Download gives you CSV file(s) to inspect or upload to your notebook environment.",
        ],
        controls: [
          {
            type: "choice",
            key: "sf_delivery",
            label: "Strategy output",
            value: "render",
            required: true,
            options: [
              { label: "Render in code block", value: "render", summary: "Generated code writes the named CSV from inline content." },
              { label: "Download CSV", value: "download", summary: "Download CSV file(s) named from the strategy." },
            ],
            help: "Choose whether generated workflows should embed this strategy or expect a downloaded CSV.",
          },
          {
            type: "action-button",
            key: "sf_download",
            label: "Download named strategy CSV",
            actionLabel: "Download",
            savedLabel: "Downloaded ✓",
            required: true,
            when: (s) => s.sf_delivery === "download",
            help: "Download the tag CSV and, when enabled, its group CSV using the strategy name.",
            action: "downloadStrategyCsv",
          },
        ],
        decisionSummary: (s) => {
          const artifact = currentStrategyArtifact(s);
          const lines = [];
          lines.push(`Strategy "${artifact.name}" is available to other workflows by name.`);
          lines.push(artifact.delivery === "download"
            ? "Download mode selected; generated code will reference the downloaded CSV."
            : "Render mode selected; generated workflows include the CSV content inline.");
          if (s.sf_download) lines.push("Strategy CSV download completed.");
          return lines.length > 0 ? lines : ["No actions taken yet."];
        },
        code: (s) => {
          const artifact = currentStrategyArtifact(s);
          if (artifact.delivery === "download") {
            const groupLine = artifact.groupPath ? `\ngroup_strategy_file_path = "${artifact.groupPath}"` : "";
            return `# Named strategy "${artifact.name}" uses downloaded CSV file(s).
# Place ${artifact.path.split("/").pop()} where your notebook can read it, or update this derived path.
strategy_file_path = "${artifact.path}"${groupLine}`;
          }
          return namedStrategyCode(artifact.name, { includeGroup: Boolean(artifact.groupPath) });
        },
      },
    ],
  fullCode(s) {
  const artifact = currentStrategyArtifact(state.settings);
  if (artifact.delivery === "download") {
    const groupLine = artifact.groupPath ? `\ngroup_strategy_file_path = "${artifact.groupPath}"` : "";
    return `# Named strategy "${artifact.name}" uses downloaded CSV file(s).
# Download ${strategySlug(artifact.name)}.csv${artifact.groupPath ? ` and ${strategySlug(artifact.groupName)}.csv` : ""}, then place them where your notebook can read them.
strategy_file_path = "${artifact.path}"${groupLine}`;
  }
  return namedStrategyCode(artifact.name, { includeGroup: Boolean(artifact.groupPath) });
},
};