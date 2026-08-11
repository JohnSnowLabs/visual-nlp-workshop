# Encapsulated PDF Template

## encapsulated_pdf_phi_vlm

Architecture: `DicomToPdf -> PdfToImage -> VLM OCR -> build_stacked_zero_shot_ner_pipeline -> PositionFinder -> ImageDrawRegions -> ImageToPdf -> DicomUpdatePdf`.

Run an inspection pass when the user wants to view `image`, `text`, `regions`, or `coordinates`. Run the final pass separately because PDF reconstruction may remove or overwrite intermediate columns.

```python
from pyspark.sql.functions import lit

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)

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
    ]
}

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
    ner_pipeline = Pipeline(stages=stages)
    empty_data = spark.createDataFrame([[""]], [input_text])
    return ner_pipeline.fit(empty_data)

vlm_prompt = "Detect and recognize text in the image, and output the text coordinates in a formatted manner."
dicom_with_prompt_df = dicom_df.withColumn("caption", lit(vlm_prompt))

dicom_to_pdf = DicomToPdf() \
    .setInputCols(["path"]) \
    .setOutputCol("pdf") \
    .setKeepInput(True)

pdf_to_image = PdfToImage() \
    .setInputCol("pdf") \
    .setOutputCol("image") \
    .setResolution(300) \
    .setCompressImage(True) \
    .setImageDimsCol("frame_dims")

caption_assembler = DocumentAssembler() \
    .setInputCol("caption") \
    .setOutputCol("caption_document")

schema_converter_assembler = ImageSchemaConverter() \
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

phi_detection_model = build_stacked_zero_shot_ner_pipeline(input_text="text", output_chunk="merged_ner_chunk")

position_finder = PositionFinder() \
    .setInputCols(["merged_ner_chunk"]) \
    .setOutputCol("coordinates") \
    .setPageMatrixCol("positions") \
    .setIgnoreSchema(False) \
    .setOcrScaleFactor(1.1)

inspection_pipeline = PipelineModel(stages=[
    dicom_to_pdf,
    pdf_to_image,
    caption_assembler,
    schema_converter_assembler,
    vlm_ocr,
    coordinate_extract,
    phi_detection_model,
    position_finder
])

inspection_df = inspection_pipeline.transform(dicom_with_prompt_df)
inspection_df.select("text", "regions", "coordinates").show(10, False)
```

Final reconstruction pass:

```python
schema_converter_internal = ImageSchemaConverter() \
    .setInputCol("image_assembler") \
    .setOutputCol("image") \
    .setOutputSchema(ImageSchemaConversion.INTERNAL) \
    .setKeepInput(False)

image_draw_regions = ImageDrawRegions() \
    .setInputCol("image") \
    .setInputRegionsCol("coordinates") \
    .setOutputCol("image_with_regions")

image_to_pdf = ImageToPdf() \
    .setInputCol("image_with_regions") \
    .setOutputCol("pdf_cleaned")

dicom_update_pdf = DicomUpdatePdf() \
    .setInputCol("path") \
    .setInputPdfCol("pdf_cleaned") \
    .setOutputCol("dicom") \
    .setKeepInput(True)

final_pipeline = PipelineModel(stages=[
    dicom_to_pdf,
    pdf_to_image,
    caption_assembler,
    schema_converter_assembler,
    vlm_ocr,
    coordinate_extract,
    phi_detection_model,
    position_finder,
    schema_converter_internal,
    image_draw_regions,
    image_to_pdf,
    dicom_update_pdf
])

result = final_pipeline.transform(dicom_with_prompt_df)
display_dicom(df=result, fields="dicom", limit=1, width=300)

def save_dicom_to_disk(dataframe, dicom_col="dicom", output_dir="/tmp/dicom_deid"):
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

saved_paths = save_dicom_to_disk(result, dicom_col="dicom", output_dir="/tmp/dicom_deid")
saved_paths[:5]
```
