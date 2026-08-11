# Pixel PHI Builder Template

## pixel_phi_builder

Use this builder first for pixel PHI workflows unless the user explicitly asks for zero-shot stacking, blanket text removal, or a hand-built custom path. This path uses Healthcare NLP `PretrainedPipeline(...)` from `models.yaml` `clinical_pipeline`.

When this route is selected, output the complete code block below. Do not show only the `dicom_pipeline_builder(...)` call-site; include all helper functions, config, `PretrainedPipeline` loading, transform call, and the inspection pass when relevant.

```python
def ocr_vlm_pipeline_builder(config, logger):
    logger.info("Building VLM OCR pipeline")

    dicom_to_image = DicomToImageV3() \
        .setInputCols(["content"]) \
        .setOutputCol("image") \
        .setCompressionMode(config["compression_mode"]) \
        .setKeepInput(False) \
        .setMemoryOptimized(config["memory_optimized"]) \
        .setCompressionQuality(config["compression_quality"]) \
        .setScale(config["scale"]) \
        .setFrameLimit(config["frame_sampling"]) \
        .setFrameSamplingStrategy(config["frame_sampling_strategy"]) \
        .setCompressImage(True) \
        .setFrameDimsCol("frame_dims")

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

    return [dicom_to_image, caption_assembler, schema_converter_assembler, vlm_ocr, coordinate_extract]

def ocr_non_vlm_pipeline_builder(config, logger):
    engine = config["ocr_engine"].lower()
    logger.info("Building non-VLM OCR pipeline: engine=%s, gpu=%s", engine, config["gpu"])

    dicom_to_image = DicomToImageV3() \
        .setInputCols(["content"]) \
        .setOutputCol("image") \
        .setCompressionMode(config["compression_mode"]) \
        .setKeepInput(False) \
        .setMemoryOptimized(config["memory_optimized"]) \
        .setCompressionQuality(config["compression_quality"]) \
        .setScale(config["scale"]) \
        .setFrameLimit(config["frame_sampling"]) \
        .setFrameSamplingStrategy(config["frame_sampling_strategy"]) \
        .setCompressImage(False) \
        .setFrameDimsCol("frame_dims")

    text_detector = ImageTextDetector.pretrained("image_text_detector_mem_opt", "en", "clinical/ocr") \
        .setInputCol("image") \
        .setOutputCol("text_regions") \
        .setScoreThreshold(0.7) \
        .setLinkThreshold(0.5) \
        .setWithRefiner(True) \
        .setTextThreshold(0.4) \
        .setSizeThreshold(-1) \
        .setUseGPU(config["gpu"]) \
        .setWidth(0)

    if engine == "v2":
        ocr = ImageToTextV2.pretrained("ocr_large_printed_v2_opt", "en", "clinical/ocr") \
            .setRegionsColumn("text_regions") \
            .setInputCols(["image"]) \
            .setOutputCol("text") \
            .setOutputFormat(OcrOutputFormat.TEXT_WITH_POSITIONS) \
            .setGroupImages(False) \
            .setKeepInput(False) \
            .setUseGPU(config["gpu"]) \
            .setUseCaching(True)
    elif engine == "v3":
        ocr = ImageToTextV3() \
            .setInputCols(["image", "text_regions"]) \
            .setOutputCol("text")
    else:
        raise ValueError(f"Unsupported OCR engine: {config['ocr_engine']!r}. Expected 'VLM', 'V2', or 'V3'.")

    return [dicom_to_image, text_detector, ocr]

def nlp_builder(spark_session, pipeline, logger):
    logger.info("Building NLP subpipeline from clinical PretrainedPipeline")

    stages = list(pipeline.model.stages)
    stage_indices = {"ner_cutoff": [], "chunk_merger": [], "document_splitter": None, "sentence_detector": None}

    for idx, stage in enumerate(stages):
        stage_name = stage.uid.rsplit("_", 1)[0].lower()
        if stage_name in {"lightdeidentification", "deidentification"}:
            stage_indices["ner_cutoff"].append(idx)
        elif stage_name == "chunkmergemodel":
            stage_indices["chunk_merger"].append(idx)
        elif stage_name == "internaldocumentsplitter":
            stage_indices["document_splitter"] = idx
        elif stage_name in {"sentencedetector", "sentencedetectordlmodel"}:
            stage_indices["sentence_detector"] = idx

    if not stage_indices["ner_cutoff"]:
        raise ValueError("The pretrained pipeline does not contain a LightDeIdentification or DeIdentification stage.")
    if not stage_indices["chunk_merger"]:
        raise ValueError("The pretrained pipeline does not contain a ChunkMergeModel stage.")
    if stage_indices["document_splitter"] is None and stage_indices["sentence_detector"] is None:
        raise ValueError("The pretrained pipeline must contain InternalDocumentSplitter, SentenceDetector, or SentenceDetectorDLModel.")

    chunk_merger_idx = max(stage_indices["chunk_merger"])
    cutoff_idx = min(stage_indices["ner_cutoff"])
    ner_chunk_output_col = stages[chunk_merger_idx].getOutputCol()
    nlp_pipeline = Pipeline(stages=stages[:cutoff_idx])
    empty_data = spark_session.createDataFrame([[""]], ["text"])
    nlp_model = nlp_pipeline.fit(empty_data)

    return ner_chunk_output_col, nlp_model

def dicom_pipeline_builder(spark_session, config, intermediate_result, pretrained_pipeline, dicom_metadata_deid=None):
    import logging

    logger = logging.getLogger("dicom_deidentification_pipeline")
    logger.setLevel(logging.INFO)

    if config["ocr_engine"].lower() == "vlm":
        ingestion_stages = ocr_vlm_pipeline_builder(config, logger)
    else:
        ingestion_stages = ocr_non_vlm_pipeline_builder(config, logger)

    ner_output, nlp_pipeline = nlp_builder(spark_session, pretrained_pipeline, logger)

    position_finder = PositionFinder() \
        .setInputCols([ner_output]) \
        .setOutputCol("coordinates") \
        .setPageMatrixCol("positions") \
        .setIgnoreSchema(False) \
        .setOcrScaleFactor(1.1)

    final_stages = [*ingestion_stages, nlp_pipeline, position_finder]

    if intermediate_result:
        return PipelineModel(stages=final_stages)

    draw_regions = DicomDrawRegions() \
        .setInputCol("path") \
        .setInputRegionsCol("coordinates") \
        .setOutputCol("dicom_pixel_cleaned") \
        .setAggCols(["path"]) \
        .setKeepInput(True) \
        .setScaleFactor(1 / config["scale"])

    if dicom_metadata_deid is None:
        dicom_deidentifier = DicomMetadataDeidentifier() \
            .setInputCols(["dicom_pixel_cleaned"]) \
            .setOutputCol("dicom_metadata_cleaned") \
            .setKeepInput(False) \
            .setStrategyFile(config["strategy_file_path"]) \
            .setRemovePrivateTags(config["remove_private_tags"])
    else:
        dicom_deidentifier = dicom_metadata_deid

    original_metadata = DicomToMetadata() \
        .setInputCol("path") \
        .setOutputCol("metadata_original") \
        .setKeepInput(True) \
        .setExtractTagForNer(False)

    final_metadata = DicomToMetadata() \
        .setInputCol("dicom_metadata_cleaned") \
        .setOutputCol("metadata_cleaned") \
        .setKeepInput(True) \
        .setExtractTagForNer(False)

    final_stages.extend([draw_regions, dicom_deidentifier, original_metadata, final_metadata])

    return PipelineModel(stages=final_stages)

from sparknlp.pretrained import PretrainedPipeline
from pyspark.sql.functions import lit
from textwrap import dedent

dicom_path = "/path/to/dicom/files"
dicom_df = spark.read.format("binaryFile").load(dicom_path)

clinical_pipeline_name = "clinical_deidentification_docwise_benchmark_medium"
vlm_prompt = "Detect and recognize text in the image, and output the text coordinates in a formatted manner."
csv_strategy_data = dedent("""\
Tags,VR,Name,Status,Action,Option
"(0010, 0010)",PN,Patient Name,,replaceWithLiteral,<REMOVED>
"(0010, 0020)",LO,Patient ID,,hashId,
"(0008, 0090)",PN,Referring Physician Name,,replaceWithLiteral,<REMOVED>
""")
strategy_file_path = "dicom_metadata_deidentification_strategy.csv"
with open(strategy_file_path, "w", encoding="utf-8", newline="") as file:
    file.write(csv_strategy_data)
print(f"Strategy file saved to: {strategy_file_path}")

config = {
    "remove_private_tags": False,
    "strategy_file_path": strategy_file_path,
    "scale": 0.75,
    "frame_sampling": 5,
    "frame_sampling_strategy": FrameSamplingStrategy.CONSECUTIVE,
    "compression_quality": 80,
    "compression_mode": "disabled",
    "memory_optimized": False,
    "gpu": True,
    "ocr_engine": "VLM",
}

deid_pipeline = PretrainedPipeline(clinical_pipeline_name, "en", "clinical/models")
dicom_with_prompt_df = dicom_df.withColumn("caption", lit(vlm_prompt))

pipeline = dicom_pipeline_builder(spark_session=spark, config=config, intermediate_result=False, pretrained_pipeline=deid_pipeline, dicom_metadata_deid=None)

result = pipeline.transform(dicom_with_prompt_df)
display_dicom(df=result, fields="dicom_metadata_cleaned", limit=1, width=300)

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

Intermediate inspection pass. This pipeline stops before `DicomDrawRegions`, so selecting `text` and `coordinates` is valid here. Do not run this selection on the final pipeline result after `DicomDrawRegions`.

```python
intermediate_pipeline = dicom_pipeline_builder(spark_session=spark, config=config, intermediate_result=True, pretrained_pipeline=deid_pipeline, dicom_metadata_deid=None)

intermediate_df = intermediate_pipeline.transform(dicom_with_prompt_df)
intermediate_df.select("text", "coordinates").show(10, False)
```
