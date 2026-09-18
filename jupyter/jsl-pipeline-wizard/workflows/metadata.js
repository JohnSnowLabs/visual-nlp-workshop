const workflowMetadata = {
  name: "DICOM Metadata-Only De-id",
  description: "De-identify structured DICOM metadata tags using a strategy CSV.",
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
          "De-identifies structured DICOM metadata tags using a strategy CSV.",
          "Supports hashing, removal, replacement, date shifting, and NER-based free-text cleaning.",
          ],
          outputs: [
          "A DICOM file with PHI metadata tags de-identified per the strategy.",
          "Optionally free-text fields cleaned via a cleanTag NER pipeline.",
          ],
          prerequisites: [
          "A strategy CSV created by the Strategy File Builder workflow.",
          "DICOM files accessible in your Spark environment.",
          ],
          whenToUse: [
          "When you need to de-identify DICOM header tags only (no pixel redaction).",
          "When running metadata-only de-id as part of a larger pipeline.",
          ],
        },
        },
      ],
      decisionSummary: () => [],
      code: () => "",
    },


    // ── Step 1: Extract Metadata ───────────────────────────────────────────
    {
      title: "DICOM Header Extraction",
      kind: "Required",
      description: "Extract DICOM headers for downstream tasks.",
      output: "Visual-NLP JSON Representation of DICOM Headers.",
      mustUnderstand: [
        "For free text de-id enable .setExtractTagForNer()",
        "This stage only extracts the headers; the actual de-id occurs separately.",
        "Make sure to mark tags with cleanTag action to enable the de-id for free text for a specific tag."
      ],
      controls: [
        {
          type: "toggle", key: "md_extractTagForNer", label: "Extract tags for NER (setExtractTagForNer)",
          value: false, required: true,
          help: "Flattens free-text tag values for NER input. Enable only when running a cleanTag NER pipeline on free-text DICOM fields.",
          impact: (v) => v
            ? "Free-text tags extracted for NER — strategy file must include cleanTag entries."
            : "Standard tag extraction only (setExtractTagForNer(False)).",
        },
      ],
      decisionSummary: (s) => [
        s.md_extractTagForNer
          ? "NER tag extraction enabled (setExtractTagForNer(True))."
          : "Standard tag extraction (setExtractTagForNer(False)).",
      ],
      code: (s) => {
        const nerActive = s.md_extractTagForNer && Array.isArray(s.md_nerModels) && s.md_nerModels.length > 0;
        const extractForNer = nerActive || s.md_extractTagForNer;
        const extraLines = nerActive
          ? ` \\\n    .setTagMappingCol("tag_mapping") \\\n    .setTagCol("tag_text") \\\n    .setStrategyFile(strategy_file_path)`
          : "";
        return `metadata = DicomToMetadata() \\
    .setInputCol("content") \\
    .setOutputCol("metadata_original") \\
    .setKeepInput(True) \\
    .setExtractTagForNer(${pyBool(extractForNer)})${extraLines}`;
      },
    },

    // ── Step 2: Clinical NLP Pipeline Selection ────────────────────────────
    {
      title: "NER Model Selection",
      kind: "Optional",
      when: (s) => Boolean(s.md_extractTagForNer),
      description: "Select 1+ NER models for free text de-id.",
      output: "Visual NLP representation of Tags and de-id tag values.",
      mustUnderstand: [
       "Multiple NER models can be combined to detect a broader range of entity types.",
       "Choose NER models that target the entity types relevant to your use case."
      ],
      controls: [
        {
          type: "multiselect",
          key: "md_nerModels",
          label: "Zero-shot NER models",
          value: ["zeroshot_ner_deid_subentity_merged_medium"],
          required: true,
          help: "Select one or more models to stack. Each model contributes its entity labels; chunks are merged before de-identification.",
          impact: (v) => {
            if (!Array.isArray(v) || v.length === 0) return "No NER models selected — step will be skipped.";
            return `${v.length} model(s) selected: ${v.join(", ")}.`;
          },
          options: [
            {
              value: "zeroshot_ner_deid_subentity_merged_medium",
              label: "Subentity Merged Medium",
              summary: "DOCTOR, PATIENT, AGE, DATE, HOSPITAL, CITY, STREET, STATE, COUNTRY, PHONE, IDNUM, EMAIL, ZIP, ORGANIZATION, PROFESSION, USERNAME",
            },
            {
              value: "zeroshot_ner_deid_subentity_merged_large",
              label: "Subentity Merged Large",
              summary: "DOCTOR, PATIENT, AGE, DATE, HOSPITAL, CITY, STREET, STATE, COUNTRY, PHONE, IDNUM, EMAIL, ZIP, ORGANIZATION, PROFESSION, USERNAME",
            },
            {
              value: "zeroshot_ner_deid_subentity_docwise_medium",
              label: "Subentity Docwise Medium",
              summary: "AGE, CITY, COUNTRY, DATE, DOCTOR, HOSPITAL, IDNUM, ORGANIZATION, PATIENT, PHONE, PROFESSION, STATE, STREET, ZIP",
            },
            {
              value: "zeroshot_ner_deid_subentity_docwise_large",
              label: "Subentity Docwise Large",
              summary: "AGE, CITY, COUNTRY, DATE, DOCTOR, HOSPITAL, IDNUM, ORGANIZATION, PATIENT, PHONE, PROFESSION, STATE, STREET, ZIP",
            },
            {
              value: "zeroshot_ner_deid_generic_docwise_medium",
              label: "Generic Docwise Medium",
              summary: "AGE, CONTACT, DATE, ID, LOCATION, NAME, PROFESSION",
            },
            {
              value: "zeroshot_ner_deid_generic_docwise_large",
              label: "Generic Docwise Large",
              summary: "AGE, CONTACT, DATE, ID, LOCATION, NAME, PROFESSION",
            },
            {
              value: "zeroshot_ner_deid_generic_nonMedical_medium",
              label: "Generic Non-Medical Medium",
              summary: "NAME, AGE, DATE, LOCATION, ID, CONTACT, PROFESSION",
            },
            {
              value: "zeroshot_ner_deid_generic_nonMedical_large",
              label: "Generic Non-Medical Large",
              summary: "NAME, AGE, DATE, LOCATION, ID, CONTACT, PROFESSION",
            },
            {
              value: "zeroshot_ner_deid_subentity_nonMedical_medium",
              label: "Subentity Non-Medical Medium",
              summary: "ACCOUNTNUM, AGE, CITY, COUNTRY, DATE, DEVICE, DLN, DOCTOR, EMAIL, GENDER, HOSPITAL, IDNUM, IP, LOCATION_OTHER, MEDICALRECORD, NAME, ORGANIZATION, PATIENT, PHONE, PLATE, PROFESSION, SSN, STATE, STREET, TIME, URL, USERNAME, VIN, ZIP",
            },
            {
              value: "zeroshot_ner_deid_subentity_nonMedical_large",
              label: "Subentity Non-Medical Large",
              summary: "ACCOUNTNUM, AGE, CITY, COUNTRY, DATE, DEVICE, DLN, DOCTOR, EMAIL, GENDER, HOSPITAL, IDNUM, IP, LOCATION_OTHER, MEDICALRECORD, NAME, ORGANIZATION, PATIENT, PHONE, PLATE, PROFESSION, SSN, STATE, STREET, TIME, URL, USERNAME, VIN, ZIP",
            },
          ],
        },
      ],
      decisionSummary: (s) => {
        const models = Array.isArray(s.md_nerModels) ? s.md_nerModels : [];
        if (models.length === 0) return ["No NER models selected — structured tag strategy only."];
        return [`${models.length} NER model(s): ${models.join(", ")}.`];
      },
      code: (s) => {
        const models = Array.isArray(s.md_nerModels) ? s.md_nerModels : [];
        if (models.length === 0) return "# Clinical NLP step skipped — no NER models selected.";

        // Entity label maps per model
        const ENTITY_LABELS = {
          "zeroshot_ner_deid_subentity_merged_medium": ["DOCTOR","PATIENT","AGE","DATE","HOSPITAL","CITY","STREET","STATE","COUNTRY","PHONE","IDNUM","EMAIL","ZIP","ORGANIZATION","PROFESSION","USERNAME"],
          "zeroshot_ner_deid_subentity_merged_large":  ["DOCTOR","PATIENT","AGE","DATE","HOSPITAL","CITY","STREET","STATE","COUNTRY","PHONE","IDNUM","EMAIL","ZIP","ORGANIZATION","PROFESSION","USERNAME"],
          "zeroshot_ner_deid_subentity_docwise_medium":["AGE","CITY","COUNTRY","DATE","DOCTOR","HOSPITAL","IDNUM","ORGANIZATION","PATIENT","PHONE","PROFESSION","STATE","STREET","ZIP"],
          "zeroshot_ner_deid_subentity_docwise_large": ["AGE","CITY","COUNTRY","DATE","DOCTOR","HOSPITAL","IDNUM","ORGANIZATION","PATIENT","PHONE","PROFESSION","STATE","STREET","ZIP"],
          "zeroshot_ner_deid_generic_docwise_medium":  ["AGE","CONTACT","DATE","ID","LOCATION","NAME","PROFESSION"],
          "zeroshot_ner_deid_generic_docwise_large":   ["AGE","CONTACT","DATE","ID","LOCATION","NAME","PROFESSION"],
          "zeroshot_ner_deid_generic_nonMedical_medium":["NAME","AGE","DATE","LOCATION","ID","CONTACT","PROFESSION"],
          "zeroshot_ner_deid_generic_nonMedical_large": ["NAME","AGE","DATE","LOCATION","ID","CONTACT","PROFESSION"],
          "zeroshot_ner_deid_subentity_nonMedical_medium":["ACCOUNTNUM","AGE","CITY","COUNTRY","DATE","DEVICE","DLN","DOCTOR","EMAIL","GENDER","HOSPITAL","IDNUM","IP","LOCATION_OTHER","MEDICALRECORD","NAME","ORGANIZATION","PATIENT","PHONE","PLATE","PROFESSION","SSN","STATE","STREET","TIME","URL","USERNAME","VIN","ZIP"],
          "zeroshot_ner_deid_subentity_nonMedical_large": ["ACCOUNTNUM","AGE","CITY","COUNTRY","DATE","DEVICE","DLN","DOCTOR","EMAIL","GENDER","HOSPITAL","IDNUM","IP","LOCATION_OTHER","MEDICALRECORD","NAME","ORGANIZATION","PATIENT","PHONE","PLATE","PROFESSION","SSN","STATE","STREET","TIME","URL","USERNAME","VIN","ZIP"],
        };

        // Build zero_shot_models config list
        const modelEntries = models.map((name) => {
          const slug = name.replace(/^zeroshot_ner_deid_/, "").replace(/_/g, "_");
          const labelVar = `${slug}_labels`;
          const outputCol = `${slug}_ner`;
          const chunkCol  = `${slug}_chunk`;
          const labels = ENTITY_LABELS[name] || [];
          return { name, slug, labelVar, outputCol, chunkCol, labels };
        });

        const labelBlocks = modelEntries.map(({ labelVar, labels }) =>
          `${labelVar} = [\n    ${labels.map((l) => `"${l}"`).join(",\n    ")},\n]`
        ).join("\n\n");

        const modelListItems = modelEntries.map(({ name, labelVar, outputCol, chunkCol }) =>
          `    {\n        "name": "${name}",\n        "labels": ${labelVar},\n        "output_col": "${outputCol}",\n        "chunk_col": "${chunkCol}",\n    }`
        ).join(",\n");

        return `${labelBlocks}

zero_shot_models = [
${modelListItems},
]

def build_stacked_zero_shot_metadata_pipeline(input_text="tag_text"):
    document_assembler = DocumentAssembler() \\
        .setInputCol(input_text) \\
        .setOutputCol("t_document")

    sentence_detector = SentenceDetector() \\
        .setInputCols(["t_document"]) \\
        .setOutputCol("t_sentence") \\
        .setCustomBounds(["<dicom>"]) \\
        .setUseCustomBoundsOnly(True)

    tokenizer = Tokenizer() \\
        .setInputCols(["t_sentence"]) \\
        .setOutputCol("t_token")

    stages = [document_assembler, sentence_detector, tokenizer]
    chunk_cols = []

    for model_settings in zero_shot_models:
        zero_shot_ner = PretrainedZeroShotNER().pretrained(model_settings["name"], "en", "clinical/models") \\
            .setInputCols(["t_sentence", "t_token"]) \\
            .setOutputCol(model_settings["output_col"]) \\
            .setPredictionThreshold(0.5) \\
            .setLabels(model_settings["labels"])

        ner_converter = NerConverterInternal() \\
            .setInputCols(["t_sentence", "t_token", model_settings["output_col"]]) \\
            .setOutputCol(model_settings["chunk_col"])

        stages.extend([zero_shot_ner, ner_converter])
        chunk_cols.append(model_settings["chunk_col"])

    chunk_merger = ChunkMergeApproach() \\
        .setInputCols(chunk_cols) \\
        .setOutputCol("t_ner_chunk")

    deid_documents = DeIdentification() \\
        .setInputCols(["t_sentence", "t_token", "t_ner_chunk"]) \\
        .setOutputCol("deid_documents") \\
        .setMode("deid")

    stages.extend([chunk_merger, deid_documents])

    nlp_pipeline = Pipeline(stages=stages)
    empty_data = spark.createDataFrame([[""]], [input_text])
    return nlp_pipeline.fit(empty_data)`;
      },
    },

    // ── Step 3: Apply Metadata De-id ───────────────────────────────────────
    {
      title: "Metadata De-Identification",
      kind: "Required",
      description: "De-Id DICOM headers.",
      output: "DICOM bytes stripped of all PHI from headers.",
      mustUnderstand: [
        "All private tags can be removed from the DICOM headers.",
        "Tags not listed in the strategy CSV remain unchanged.",
        "The strategy file defines the actions this stage should apply to each tag.",
        "The group strategy file defines actions for entire groups of DICOM tags rather than individual tags.",
        "Group Strategy files only support remove/delete actions."
      ],
      controls: [
        {
          type: "toggle", key: "md_removePrivateTags", label: "Remove private tags",
          value: false, required: true,
          help: "Private (vendor-specific) tags may contain PHI not covered by standard tag strategies.",
          impact: (v) => v ? "Private tags will be removed." : "Private tags preserved (vendor data intact).",
        },
        {
          type: "toggle", key: "md_includeGroupStrategy", label: "Include group strategy file",
          value: false, required: true,
          help: "Apply a group-level strategy CSV in addition to the tag-level strategy. Useful for covering entire tag groups by prefix.",
          impact: (v) => v ? "Group strategy file will be included via setGroupStrategyFile." : "No group strategy.",
        },
      ],
      decisionSummary: (s) => [
        s.md_removePrivateTags ? "Private tags: removed." : "Private tags: preserved.",
        s.md_includeGroupStrategy ? "Group strategy: included." : "No group strategy.",
      ],
      code: (s) => {
        const nerActive = s.md_extractTagForNer && Array.isArray(s.md_nerModels) && s.md_nerModels.length > 0;
        const groupLine = s.md_includeGroupStrategy
          ? ` \\\n    .setGroupStrategyFile(group_strategy_file_path)`
          : "";
        const nerLines = nerActive
          ? ` \\\n    .setTagMappingCol("tag_mapping") \\\n    .setTagCleanedCol("deid_documents")`
          : "";
        return `dicom_deidentifier = DicomMetadataDeidentifier() \\
    .setInputCols(["path"]) \\
    .setOutputCol("${FIXED.metadataOutputCol}") \\
    .setKeepInput(True) \\
    .setStrategyFile(strategy_file_path) \\
    .setRemovePrivateTags(${pyBool(s.md_removePrivateTags)})${nerLines}${groupLine}`;
      },
    },

    // ── Step 4: Compare and Validate ──────────────────────────────────────
    {
      title: "Compare and Validate",
      kind: "Required",
      description: "Extract cleaned metadata, compare before/after, and save de-identified DICOM files.",
      output: "Side-by-side comparison DataFrame and saved de-identified DICOM files.",
      mustUnderstand: [
        "The comparison reads back cleaned tags from `dicom_metadata_cleaned` via a second `DicomToMetadata` call.",
        "Inspect the comparison DataFrame before calling `save_dicom_to_disk`.",
        "`display_dicom` shows the cleaned DICOM inline; use `saved_paths` to verify output paths.",
      ],
      controls: [
        {
          type: "choice", key: "includeSaveHelper", label: "Include Dicom Disk Saver Function",
          value: "yes",
          options: [
            { label: "Yes", value: "yes", summary: "Includes the save_dicom_to_disk() helper function and its call site." },
            { label: "No",  value: "no",  summary: "Omits the save helper — only comparison and display are shown." },
          ],
          help: "Choose Yes to include the save_dicom_to_disk() function definition and its call in the code block.",
        },
      ],
      decisionSummary: (s) => [
        "Side-by-side comparison of original and cleaned metadata.",
        s.includeSaveHelper !== "no" ? "Save helper included." : "Save helper omitted.",
      ],
      code: (s) => {
        const col = FIXED.metadataOutputCol;
        return `# Extract cleaned metadata for before/after comparison
metadata_cleaned_stage = DicomToMetadata() \\
    .setInputCol("${col}") \\
    .setOutputCol("metadata_cleaned") \\
    .setKeepInput(True) \\
    .setExtractTagForNer(False)`;
      },
      postCode: (s) => {
        const col = FIXED.metadataOutputCol;
        const includeSave = s.includeSaveHelper !== "no";
        const saveFn = includeSave ? `def save_dicom_to_disk(dataframe, dicom_col="${col}", output_dir="${FIXED.outputDir}"):
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

    return saved_paths

` : "";
        const saveCall = includeSave
          ? `\nsaved_paths = save_dicom_to_disk(result, dicom_col="${col}", output_dir="${FIXED.outputDir}")\nsaved_paths[:5]`
          : "";
        return `${saveFn}display_dicom(df=result, fields="${col}", limit=1, width=300)${saveCall}`;
      },
    },
  ],

  fullCode(s) {
    const steps = this.steps;
    const byTitle = (t) => steps.find((st) => st.title === t);
    const extractStep  = byTitle("DICOM Header Extraction");
    const nerStep      = byTitle("NER Model Selection");
    const deidStep     = byTitle("Metadata De-Identification");
    const validateStep = byTitle("Compare and Validate");

    const nerActive = s.md_extractTagForNer && Array.isArray(s.md_nerModels) && s.md_nerModels.length > 0;

    const nerCode    = nerActive && nerStep ? nerStep.code(s) : "";
    const nerPipeline = nerActive
      ? `metadata_nlp_pipeline = build_stacked_zero_shot_metadata_pipeline(input_text="tag_text")\n`
      : "";

    const outerStages = nerActive
      ? `stages = [metadata, metadata_nlp_pipeline, dicom_deidentifier, metadata_cleaned_stage]`
      : `stages = [metadata, dicom_deidentifier, metadata_cleaned_stage]`;

    const groupPathLine = s.md_includeGroupStrategy
      ? `group_strategy_file_path = "/tmp/dicom_metadata_group_strategy.csv"\n\n`
      : "";

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

${nerCode ? nerCode + "\n" : ""}
strategy_file_path = "/tmp/dicom_metadata_clean_tag_strategy.csv"

${extractStep ? extractStep.code(s) : ""}

${groupPathLine}${deidStep ? deidStep.code(s) : ""}

${validateStep ? validateStep.code(s) : ""}

${nerPipeline}${outerStages}
pipeline = Pipeline(stages=stages)

dicom_path = "${FIXED.dicomPath}"
dicom_df = spark.read.format("binaryFile").load(dicom_path)
result = pipeline.fit(dicom_df).transform(dicom_df).cache()

comparison_df = build_metadata_comparison_df(result)
comparison_df.show(truncate=False)

${validateStep ? validateStep.postCode(s) : ""}`;
  },
};
