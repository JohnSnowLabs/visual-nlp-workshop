# Zero-Shot Templates

Use this file only when routing selects `metadata_clean_tag_ner` or `pixel_phi_zero_shot`, or when the user explicitly asks for zero-shot, stacked models, or configurable entity coverage.

Read `models.yaml` `zero_shot_ner_models.options` for model names, model-card links, and entity lists. Do not duplicate the model catalog here.

## Shared Config Rule

Before generating user code, start with this copy-ready default `config` from `models.yaml`. The labels below are only a starter example for the default model; the full zero-shot model catalog, links, and entity lists live in `models.yaml`. Add more model entries only when the user asks to stack more models or customize entity coverage.

```python
subentity_merged_medium_labels = [
    "DOCTOR",
    "PATIENT",
    "AGE",
    "DATE",
    "HOSPITAL",
    "CITY",
    "STREET",
    "STATE",
    "COUNTRY",
    "PHONE",
    "IDNUM",
    "EMAIL",
    "ZIP",
    "ORGANIZATION",
    "PROFESSION",
    "USERNAME",
]

config = {
    "zero_shot_models": [
        {
            "name": "zeroshot_ner_deid_subentity_merged_medium",
            "labels": subentity_merged_medium_labels,
            "output_col": "subentity_merged_medium_ner",
            "chunk_col": "subentity_merged_medium_chunk",
        }
    ],
    "scale": 0.75,
    "frame_sampling": 5,
    "frame_sampling_strategy": FrameSamplingStrategy.CONSECUTIVE,
}
```

## metadata_clean_tag_ner

Use `build_stacked_zero_shot_metadata_pipeline(...)` for this workflow. Do not use `build_stacked_zero_shot_ner_pipeline(...)` for metadata cleanTag.

```python
from textwrap import dedent

csv_clean_tag_data = dedent("""\
Tags,VR,Name,Status,Action,Option
"(0008, 1030)",LO,Study Description,,cleanTag,deid
"(0018, 1030)",LO,Protocol Name,,cleanTag,deid
"(0040, 4000)",LT,Comments on the Performed Procedure Step,,cleanTag,deid
""")

strategy_file_path = "dicom_metadata_clean_tag_strategy.csv"

with open(strategy_file_path, "w", encoding="utf-8", newline="") as file:
    file.write(csv_clean_tag_data)

print(f"Strategy file saved to: {strategy_file_path}")

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)

def build_stacked_zero_shot_metadata_pipeline(input_text="tag_text"):
    document_assembler = DocumentAssembler() \
        .setInputCol(input_text) \
        .setOutputCol("t_document")

    sentence_detector = SentenceDetector() \
        .setInputCols(["t_document"]) \
        .setOutputCol("t_sentence") \
        .setCustomBounds(["<dicom>"]) \
        .setUseCustomBoundsOnly(True)

    tokenizer = Tokenizer() \
        .setInputCols(["t_sentence"]) \
        .setOutputCol("t_token")

    stages = [document_assembler, sentence_detector, tokenizer]
    chunk_cols = []

    for model_settings in config["zero_shot_models"]:
        zero_shot_ner = PretrainedZeroShotNER().pretrained(model_settings["name"], "en", "clinical/models") \
            .setInputCols(["t_sentence", "t_token"]) \
            .setOutputCol(model_settings["output_col"]) \
            .setPredictionThreshold(0.5) \
            .setLabels(model_settings["labels"])

        ner_converter = NerConverterInternal() \
            .setInputCols(["t_sentence", "t_token", model_settings["output_col"]]) \
            .setOutputCol(model_settings["chunk_col"])

        stages.extend([zero_shot_ner, ner_converter])
        chunk_cols.append(model_settings["chunk_col"])

    chunk_merger = ChunkMergeApproach() \
        .setInputCols(chunk_cols) \
        .setOutputCol("t_ner_chunk")

    deid_documents = DeIdentification() \
        .setInputCols(["t_sentence", "t_token", "t_ner_chunk"]) \
        .setOutputCol("deid_documents") \
        .setMode("deid")

    stages.extend([chunk_merger, deid_documents])

    nlp_pipeline = Pipeline(stages=stages)
    empty_data = spark.createDataFrame([[""]], [input_text])
    return nlp_pipeline.fit(empty_data)

dicom_to_metadata = DicomToMetadata() \
    .setInputCol("content") \
    .setOutputCol("metadata_original") \
    .setKeepInput(False) \
    .setExtractTagForNer(True) \
    .setTagMappingCol("tag_mapping") \
    .setTagCol("tag_text") \
    .setStrategyFile(strategy_file_path)

dicom_deidentifier = DicomMetadataDeidentifier() \
    .setInputCols(["path"]) \
    .setOutputCol("dicom_metadata_cleaned") \
    .setTagMappingCol("tag_mapping") \
    .setTagCleanedCol("deid_documents") \
    .setKeepInput(True) \
    .setStrategyFile(strategy_file_path)

deid_metadata = DicomToMetadata() \
    .setInputCol("dicom_metadata_cleaned") \
    .setOutputCol("metadata_cleaned") \
    .setKeepInput(True) \
    .setExtractTagForNer(False)

pipeline = PipelineModel(stages=[dicom_to_metadata, build_stacked_zero_shot_metadata_pipeline(input_text="tag_text"), dicom_deidentifier, deid_metadata])

result = pipeline.transform(dicom_df)
display_dicom(df=result, fields="dicom_metadata_cleaned", limit=1, width=300)

def build_metadata_comparison_df(dataframe, original_col="metadata_original", cleaned_col="metadata_cleaned"):
    import json
    import pandas as pd

    collect_result = []

    for row in dataframe.select("path", original_col, cleaned_col).toLocalIterator():
        data = row.asDict()
        metadata_original = json.loads(data[original_col])
        metadata_cleaned = json.loads(data[cleaned_col])

        for tag in metadata_original.keys():
            original_value = metadata_original[tag]["value"]
            cleaned_value = "DELETED" if tag not in metadata_cleaned else metadata_cleaned[tag]["value"]
            value_changed = False if original_value == cleaned_value else True
            value_deleted = True if original_value != "DELETED" and cleaned_value == "DELETED" else False
            collect_result.append([tag, metadata_original[tag]["vr"], original_value, cleaned_value, value_changed, value_deleted])

    columns = ["Tag", "VR", "Original_Value", "Cleaned_Value", "Is_Changed", "Is_Deleted"]
    metadata_result_df = pd.DataFrame(collect_result, columns=columns)
    return metadata_result_df

metadata_result_df = build_metadata_comparison_df(result)
metadata_result_df.head()

def save_dicom_to_disk(dataframe, dicom_col="dicom_metadata_cleaned", output_dir="/tmp/dicom_deid"):
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

saved_paths = save_dicom_to_disk(result, dicom_col="dicom_metadata_cleaned", output_dir="/tmp/dicom_deid")
saved_paths[:5]
```

## pixel_phi_zero_shot

This template is the zero-shot alternative to the builder. Do not call `dicom_pipeline_builder(...)` here. Use `build_stacked_zero_shot_ner_pipeline(...)` only for pixel de-identification without the pipeline builder.

`DicomDrawRegions` is an aggregation stage. Use a separate inspection pipeline that stops before `DicomDrawRegions` if the user wants `text`, `regions`, or `coordinates`; after this final stage, validate with `display_dicom` on `dicom_pixel_cleaned`.

```python
from pyspark.sql.functions import lit

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)

def build_stacked_zero_shot_ner_pipeline(input_text="text", output_chunk="merged_ner_chunk"):
    document_assembler = DocumentAssembler() \
        .setInputCol(input_text) \
        .setOutputCol("document")

    sentence_detector = SentenceDetector() \
        .setInputCols(["document"]) \
        .setOutputCol("sentence")

    tokenizer = Tokenizer() \
        .setInputCols(["sentence"]) \
        .setOutputCol("token")

    stages = [document_assembler, sentence_detector, tokenizer]
    chunk_cols = []

    for model_settings in config["zero_shot_models"]:
        zero_shot_ner = PretrainedZeroShotNER().pretrained(model_settings["name"], "en", "clinical/models") \
            .setInputCols(["sentence", "token"]) \
            .setOutputCol(model_settings["output_col"]) \
            .setPredictionThreshold(0.5) \
            .setLabels(model_settings["labels"])

        ner_converter = NerConverterInternal() \
            .setInputCols(["sentence", "token", model_settings["output_col"]]) \
            .setOutputCol(model_settings["chunk_col"])

        stages.extend([zero_shot_ner, ner_converter])
        chunk_cols.append(model_settings["chunk_col"])

    chunk_merger = ChunkMergeApproach() \
        .setInputCols(chunk_cols) \
        .setOutputCol(output_chunk)

    stages.append(chunk_merger)

    nlp_pipeline = Pipeline(stages=stages)
    empty_data = spark.createDataFrame([[""]], [input_text])
    return nlp_pipeline.fit(empty_data)

vlm_prompt = "Detect and recognize text in the image, and output the text coordinates in a formatted manner."
dicom_with_prompt_df = dicom_df.withColumn("caption", lit(vlm_prompt))

dicom_to_image = DicomToImageV3() \
    .setInputCols(["content"]) \
    .setOutputCol("image") \
    .setKeepInput(False) \
    .setScale(config["scale"]) \
    .setFrameLimit(config["frame_sampling"]) \
    .setFrameSamplingStrategy(config["frame_sampling_strategy"]) \
    .setCompressImage(True) \
    .setFrameDimsCol("frame_dims")

caption_assembler = DocumentAssembler() \
    .setInputCol("caption") \
    .setOutputCol("caption_document")

image_assembler = ImageSchemaConverter() \
    .setInputCol("image") \
    .setOutputCol("image_assembler") \
    .setOutputSchema(ImageSchemaConversion.ASSEMBLER) \
    .setKeepInput(False)

vlm_ocr = MedicalVisionLLM.pretrained("jsl-ocr-gguf-vlm1", "en", "clinical/ocr") \
    .setInputCols(["caption_document", "image_assembler"]) \
    .setOutputCol("completions") \
    .setNGpuLayers(99) \
    .setNCtx(32768) \
    .setNParallel(1) \
    .setNBatch(2048) \
    .setNUbatch(1024) \
    .setNPredict(1024) \
    .setTemperature(0.01) \
    .setTopK(1) \
    .setTopP(1.0) \
    .setRepeatPenalty(1.03) \
    .setRepeatLastN(256) \
    .setMinKeep(0) \
    .setNProbs(0) \
    .setBatchSize(1) \
    .setDisableLog(False)

coordinate_extract = DocumentCoordinatesToText() \
    .setInputCol("completions") \
    .setImageDimsCol("frame_dims") \
    .setOutputCol("text") \
    .setPageMatrixCol("positions") \
    .setRegionCol("regions") \
    .setLineTolerance(5)

position_finder = PositionFinder() \
    .setInputCols(["merged_ner_chunk"]) \
    .setOutputCol("coordinates") \
    .setPageMatrixCol("positions") \
    .setIgnoreSchema(False) \
    .setOcrScaleFactor(1.1)

draw_regions = DicomDrawRegions() \
    .setInputCol("path") \
    .setInputRegionsCol("coordinates") \
    .setOutputCol("dicom_pixel_cleaned") \
    .setAggCols(["path"]) \
    .setKeepInput(True) \
    .setScaleFactor(1 / config["scale"])

pipeline = PipelineModel(stages=[
    dicom_to_image,
    caption_assembler,
    image_assembler,
    vlm_ocr,
    coordinate_extract,
    build_stacked_zero_shot_ner_pipeline(input_text="text", output_chunk="merged_ner_chunk"),
    position_finder,
    draw_regions
])

result = pipeline.transform(dicom_with_prompt_df)
display_dicom(df=result, fields="dicom_pixel_cleaned", limit=1, width=300)

def save_dicom_to_disk(dataframe, dicom_col="dicom_pixel_cleaned", output_dir="/tmp/dicom_deid"):
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

saved_paths = save_dicom_to_disk(result, dicom_col="dicom_pixel_cleaned", output_dir="/tmp/dicom_deid")
saved_paths[:5]
```
