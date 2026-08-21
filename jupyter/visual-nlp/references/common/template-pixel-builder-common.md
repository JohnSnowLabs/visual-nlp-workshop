# Pixel Builder Common Template

Use this file for helpers shared by DICOM, image, and PDF Pixel Builder routes.

Do not put DICOM image extraction, image/PDF image extraction, drawing, display, save, metadata de-identification, or PDF reconstruction code here. Keep those in the selected input-specific Pixel Builder template.

## common_pixel_builder_helpers

Include this section before the selected Pixel Builder implementation section.

### Shared NLP Helper

```python
def nlp_builder(spark_session, pipeline):

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
```
