from pyspark.ml import PipelineModel, Pipeline
from sparkocr.transformers import *
from sparkocr.enums import *
import logging
import copy
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AnonymizationPipelineBuilder:

    """
    Builder class for constructing Spark OCR + Spark NLP Healthcare
    deidentification or obfuscation pipelines.

    This class automates the process of creating an end-to-end pipeline for
    processing PDF or image inputs, extracting text via OCR, applying clinical
    deidentification models, and generating redacted/obfuscated outputs.

    The pipeline consists of:
        - Pre-NLP stages (OCR and text preparation)
        - Pretrained Healthcare NLP pipeline
        - Post-NLP stages (deidentification, coordinate finding, visualization, PDF/image reconstruction)

    Inputs:
        hc_pipeline (str): Name of the pretrained Healthcare NLP pipeline.
        language (str): Language of the pretrained pipeline (default: "en").
        remote_loc (str): Remote location for models (default: "clinical/models").
        task (str): Either "deid" (deidentification) or "obfuscate".
        input_type (str): Input type, either "pdf" or "image".
        ocr (str): OCR version, one of ["v1", "v2", "v3"].
        use_gpu (bool): Whether to use GPU acceleration for OCR.
        keep_memory (bool): Whether to preserve auxiliary mappings for consistent obfuscation.
        default_matcher_threshold (float): Default threshold for entity matchers.
        matcher_dict (dict): Dictionary of entity-specific thresholds {ENTITY: threshold}.
        pdf_finalizer (bool): Whether to include image_to_pdf destroying intermediate results.

    Raises:
        ValueError: If invalid arguments are passed or pretrained pipeline contains unsupported stages.
        Exception: If pretrained pipeline cannot be loaded.

    Example:
        >>> builder = AnonymizationPipelineBuilder(
        ...     hc_pipeline="clinical_deidentification",
        ...     input_type="pdf",
        ...     task="obfuscate",
        ...     ocr="v1",
        ...     use_gpu=False,
        ...     keep_memory=True,
        ...     default_matcher_threshold=0.7,
        ...     matcher_dict={"NAME":0.9, "DATE":0.8}
        ... )
        >>>
        >>> pipeline_model = builder.build()
        >>>
        >>> # Load input PDFs
        >>> input_df = spark.read.format("binaryFile").load("/content/*.pdf")
        >>> result_df = pipeline_model.fit(input_df).transform(input_df)
        >>>
        >>> # Display results
        >>> from sparkocr.utils import display_pdf
        >>> display_pdf(result_df, "pdf")

    End-to-end usage with configuration update and PDF display:

        >>> builder = AnonymizationPipelineBuilder(
        ...     hc_pipeline="clinical_deidentification_docwise_wip_v2",
        ...     input_type="pdf",
        ...     ocr="v1",
        ...     use_gpu=False,
        ...     language="en",
        ...     task="deid",
        ...     keep_memory=True,
        ...     default_matcher_threshold=0.5,
        ...     remote_loc="clinical/models"
        ... )
        >>>
        >>> # Update configuration
        >>> config = builder.get_config()
        >>> config["pdfToImage"]["imageType"] = ImageType.TYPE_3BYTE_BGR
        >>> config["imageDrawRegions"]["rectColor"] = Color.red
        >>> builder.update_config(config)
        >>>
        >>> # Build pipeline
        >>> pipeline_model = builder.build()
        >>>
        >>> # Load input PDFs
        >>> input_df = spark.read.format("binaryFile").load("/content/*.pdf")
        >>> result_df = pipeline_model.fit(input_df).transform(input_df)
        >>>
        >>> # Display results
        >>> from sparkocr.utils import display_pdf
        >>> display_pdf(result_df, "pdf")
    """

    __allowed_kwargs = {"ocr", "keep_memory", "use_gpu", "default_matcher_threshold", "matcher_dict", "pdf_finalizer"}

    individual_stage_settings = {
        "pdfToImage" : {"resolution" : 300, "splitBatchNum" : 10, "imageType" : ImageType.TYPE_BYTE_GRAY, "splittingStrategy": SplittingStrategy.FIXED_NUMBER_OF_PARTITIONS},

        "binaryToImage" : {"imageType" : ImageType.TYPE_BYTE_GRAY},

        "imageToText" : {"ignoreResolution" : False, "pageIteratorLevel" : PageIteratorLevel.SYMBOL,
                         "pageSegMode" : PageSegmentationMode.SPARSE_TEXT, "withSpaces" : False,
                         "keepLayout" : False, "threshold" : 70},

        "imageTextDetectorCraft" : {"checkpoint" : "image_text_detector_mem_opt", "scoreThreshold" : 0.7, "linkThreshold" : 0.5, "textThreshold" : 0.4,
                                    "sizeThreshold" : -1, "width" : 0, "height" : 0, "refiner" : True},

        "imageToTextV2" : {"checkpoint" : "ocr_base_printed_v2_opt", "groupImages" : False, "batchSize" : 4, "taskParallelism" : 8},

        "deidentification" : {"keepMonth" : True, "keepYear" : True, "obfuscateDate" : True, "sameEntityThreshold" : 0.7, "keepTextSizeForObfuscation" : True,
                              "fakerLengthOffset" : 2, "returnEntityMappings" : True, "days" : 2, "ignoreRegex" : False, "region" : "us", "seed" : 40,
                              "consistentObfuscation" : True},

        "positionFinder" : {"smoothCoordinates" : True},

        "imageDrawRegions" : {"rectColor" : Color.black, "fontStyle" : FontStyle.PLAIN, "fontName" : "SansSerif", "patchTextColor" : Color.black, "patchBackgroundColor" : Color.white,
            "redactionList" : [], "blackList" : [], "dynamicColor" : True}
    }

    def __init__(self, hc_pipeline="", language="en", remote_loc="clinical/models", task="deid", input_type="pdf", **kwargs):
        logger.info("Initializing DeidentificationPipelineBuilder...")

        self.hc_pipeline = hc_pipeline
        self.language = language
        self.remote_loc = remote_loc
        self.task = task
        self.input_type = input_type
        self.stage_config = copy.deepcopy(type(self).individual_stage_settings)

        invalid = set(kwargs) - self.__allowed_kwargs

        if invalid:
            logger.error(f"Invalid keyword arguments: {invalid}")
            raise ValueError(f"Invalid keyword arguments: {invalid}.\nAllowed Keyword arguments: {self.__allowed_kwargs}")
        else:
            for key, value in kwargs.items():
                setattr(self, key, value)

        # DEFAULTS
        self.ocr = getattr(self, "ocr", "v1")
        self.use_gpu = getattr(self, "use_gpu", False)
        self.keep_memory = getattr(self, "keep_memory", False)
        self.default_matcher_threshold = getattr(self, "default_matcher_threshold", 0.7)
        self.matcher_dict = getattr(self, "matcher_dict", {})
        self.pdf_finalizer = getattr(self, "pdf_finalizer", True)

        if not isinstance(self.pdf_finalizer, bool):
            raise ValueError(f"pdf_finalizer must be a boolean value. pdf_finalizer of type -> {type(self.pdf_finalizer)} is not allowed.")

        if self.task not in ["deid", "obfuscate"]:
            raise ValueError("task must be either deid or obfuscate")

        if self.input_type not in ["pdf", "image"]:
            raise ValueError("input_type must be either pdf or image")

        if self.ocr not in ["v1", "v2", "v3"]:
            raise ValueError(f"ocr version must be in ['v1', 'v2', 'v3']. ocr -> {self.ocr} is not allowed.")

        if not isinstance(self.use_gpu, bool):
            raise ValueError(f"use_gpu must be a boolean value. use_gpu of type -> {type(self.use_gpu)} is not allowed.")

        if not isinstance(self.keep_memory, bool):
            raise ValueError(f"keep_memory must be a boolean value. keep_memory of type -> {type(self.keep_memory)} is not allowed.")

        if not isinstance(self.language, str):
            raise ValueError(f"language must be a string value. language of type -> {type(self.language)} is not allowed.")

        if not isinstance(self.remote_loc, str):
            raise ValueError(f"remote_loc must be a string value. remote_loc of type -> {type(self.remote_loc)} is not allowed.")

        if not (isinstance(self.default_matcher_threshold, float)) and 0.0 < self.default_matcher_threshold < 1.0:
            raise ValueError(f"default_matcher_threshold must be a float value between 0.0 and 0.99. default_matcher_threshold of type -> {type(self.default_matcher_threshold)} is not allowed.")

        if not isinstance(self.matcher_dict, dict):
            raise ValueError(f"matcher_dict must be a dictionary value. matcher_dict of type -> {type(self.matcher_dict)} is not allowed.")

        try:
            from sparknlp.pretrained import PretrainedPipeline
            self.pipeline = PretrainedPipeline(self.hc_pipeline, self.language, self.remote_loc)
            logger.info(f"Loaded pretrained pipeline: {self.hc_pipeline}")

        except Exception as E:
            logger.exception("Failed to load pretrained pipeline.")

            raise Exception(f"""Unable to Download/Load Pretrained Pipeline {E}
                Check if the pretrained pipeline has a model card page hc_pipeline, 
                language and remote_loc should match.""")

        self.check_pretrained_pipeline()
        self.validate_matchers()

    @staticmethod
    def get_names_from_uid(stages):
        """
        Extracts normalized stage names from Spark ML pipeline stages.

        Args:
            stages (list): List of Spark ML pipeline stages.

        Returns:
            list[str]: Normalized (lowercase) stage names.
        """

        return ["_".join(stage.uid.split("_")[:-1]).lower() for stage in stages]

    def validate_matchers(self):
        """
        Validates matcher dictionary thresholds and entity naming rules.

        Raises:
            ValueError: If thresholds are outside (0.0, 1.0) or entities are not uppercase.
        """

        if self.keep_memory and len(self.matcher_dict.keys()) > 0:
            for entity, thresh in self.matcher_dict.items():
                if not 0.0 < thresh < 1.0:
                    raise ValueError(f"Invalid threshold {thresh} for entity '{entity}'. Threshold must be greater than 0.0 and less than or equal to 0.99.")
                if not(entity.isupper()):
                    raise ValueError(f"Entity '{entity}' must be in uppercase.")


    def get_matcher_entities(self):
        """
        Builds matcher dictionary for the deidentification stage.

        Uses PipelineTracer to extract possible entities from the pretrained pipeline
        and applies either custom thresholds (from matcher_dict) or the default threshold.

        Returns:
            dict: {ENTITY: threshold} mapping for all detected entities.
        """

        from sparknlp_jsl.pipeline_tracer import PipelineTracer
        tracer = PipelineTracer(self.pipeline)
        entities = [entity.upper() for entity in tracer.getPossibleEntities()]

        matchers = {}
        for entity in entities:
            if entity.upper() in self.matcher_dict.keys():
                matchers[entity] = self.matcher_dict[entity]
            else:
                matchers[entity] = self.default_matcher_threshold

        logger.info(f"MatcherList For Deidentification : {matchers}")
        return matchers


    def check_pretrained_pipeline(self):
        """
        Validates that the pretrained pipeline contains only NLP components.

        Ensures no visual/image-related or DICOM stages are included.

        Raises:
            ValueError: If visual or DICOM stages are detected in the pipeline.
        """

        logger.info("Checking pretrained pipeline for visual components...")

        stages = self.get_names_from_uid(self.pipeline.model.stages)

        basic_visual_stages = ["pdftoimage", "imagetotext", "imagetextdetectorcraft", "imagetextdetectorv2", "imagetotextv2", "imagetotextv3", "neroutputcleaner", "positionfinder", "imagedrawregions", "imagetopdf"]
        basic_dicom_stages = ["dicomtoimage", "dicomtoimagev2", "dicomtoimagev3", "dicomdrawregions", "dicommetadatadeidentifier", "dicomdeidentifier", "dicomtometadata"]

        forbidden_stages = set(basic_visual_stages + basic_dicom_stages)
        found_visual_components = list(set(stages).intersection(forbidden_stages))

        if len(found_visual_components) > 0:
            logger.exception("Failed to validate pretrained pipeline as hc.")

            raise ValueError(f"""Invalid pretrained pipeline: {self.hc_pipeline} visual stages found. 
                Pretrained NLP pipeline should not contain image-related components. 
                Detected visual components: {', '.join(found_visual_components)}.
                Please check/pass the pretrained pipeline.""")
        else:
            logger.info("Pipeline check passed. Only NLP stages found.")


    def get_config(self):
        """
        Retrieves the current stage configuration.

        Returns:
            dict: Deep copy of `stage_config`.
        """

        logger.info("Retrieving individual_stage_settings...")
        return copy.deepcopy(self.stage_config)


    def update_config(self, config):
        """
        Updates the stage configuration.

        Args:
            config (dict): New configuration dictionary to replace current settings.
        """

        logger.info("Updating individual_stage_settings...")
        self.stage_config = copy.deepcopy(config)


    def reset_config(self):
        """
        Resets the stage configuration back to class defaults.
        """

        logger.info("Resetting stage_config to class defaults...")
        self.stage_config = copy.deepcopy(type(self).individual_stage_settings)


    def nlp_builder(self):
        """
        Extracts NLP stages from the pretrained pipeline.

        Raises:
            ValueError: If required stages (e.g., DocumentAssembler, RegexTokenizer) are missing.
        """

        logger.info("Building NLP stages...")

        self.hc_output_col = None
        self.document_assember_output_col = None
        self.sentence_detector_output_col = None
        self.regex_tokenizer_output_col = None

        counter = 0
        pretrained_stages = self.get_names_from_uid(self.pipeline.model.stages)
        for stage in pretrained_stages:
            if stage == "de-identification" or stage == "finisher" or stage == "lightdeidentification":
                break
            else:
                counter += 1

        self.stages += self.pipeline.model.stages[:counter]
        self.hc_output_col = self.stages[-1].getOrDefault(self.stages[-1].getParam("outputCol"))

        if self.keep_memory:

            document_candidates = ["documentassembler", "internaldocumentsplitter"]
            for cand in document_candidates:
                if cand in pretrained_stages:
                    idx_ = pretrained_stages.index(cand)
                    self.document_assember_output_col = self.stages[idx_].getOrDefault(self.stages[idx_].getParam("outputCol"))

            sentence_candidates = ["sentencedetectordlmodel", "sentencedetector"]
            for cand in sentence_candidates:
                if cand in pretrained_stages:
                    idx_ = pretrained_stages.index(cand)
                    self.sentence_detector_output_col = self.stages[idx_].getOrDefault(self.stages[idx_].getParam("outputCol"))

            tokenizer_candidates = ["regex_tokenizer", "tokenizer"]
            tokenizer_indexes = []
            for cand in tokenizer_candidates:
                if cand in pretrained_stages:
                    idx_ = len(pretrained_stages) - 1 - pretrained_stages[::-1].index(cand)
                    tokenizer_indexes.append(idx_)

            if tokenizer_indexes:
                last_idx = max(tokenizer_indexes)
                self.regex_tokenizer_output_col = (self.stages[last_idx].getOrDefault(self.stages[last_idx].getParam("outputCol")))

            if self.document_assember_output_col is None:
                raise ValueError("Bad pretrained pipeline, DocumentAssembler stage is missing.")

            if self.sentence_detector_output_col is None:
                logger.info("SentenceDetectorDL/SentenceDetector stage is missing. DeIdentification stage will use document stage output instead of sentences.")

            if self.regex_tokenizer_output_col is None:
                raise ValueError("Bad pretrained pipeline, RegexTokenizer stage is missing.")


    def pre_nlp_stages(self):
        """
        Constructs pre-NLP OCR stages depending on input type and OCR version.

        Returns:
            None (updates `self.stages` internally).
        """

        logger.info("Building pre-NLP stages...")

        stages = []

        if self.input_type == "pdf":
            pdf_to_image = PdfToImage() \
                .setInputCol("content") \
                .setSplitNumBatch(self.stage_config["pdfToImage"]["splitBatchNum"]) \
                .setOutputCol("image_raw") \
                .setImageType(self.stage_config["pdfToImage"]["imageType"]) \
                .setSplittingStategy(self.stage_config["pdfToImage"]["splittingStrategy"]) \
                .setResolution(self.stage_config["pdfToImage"]["resolution"]) \
                .setKeepInput(False)

            stages.append(pdf_to_image)

        else:
            bin_to_image = BinaryToImage() \
                .setInputCol("content") \
                .setOutputCol("image_raw") \
                .setImageType(self.stage_config["binaryToImage"]["imageType"])


            stages.append(bin_to_image)

        if self.ocr == "v1":
            ocr = ImageToText() \
                .setInputCol("image_raw") \
                .setOutputCol("text") \
                .setIgnoreResolution(self.stage_config["imageToText"]["ignoreResolution"]) \
                .setPageIteratorLevel(self.stage_config["imageToText"]["pageIteratorLevel"]) \
                .setPageSegMode(self.stage_config["imageToText"]["pageSegMode"]) \
                .setWithSpaces(self.stage_config["imageToText"]["withSpaces"]) \
                .setKeepLayout(self.stage_config["imageToText"]["keepLayout"]) \
                .setConfidenceThreshold(self.stage_config["imageToText"]["threshold"])


            stages.append(ocr)

        else:
            text_detector = ImageTextDetector.pretrained(self.stage_config["imageTextDetectorCraft"]["checkpoint"], "en", "clinical/ocr") \
                .setInputCol("image_raw") \
                .setOutputCol("text_regions") \
                .setScoreThreshold(self.stage_config["imageTextDetectorCraft"]["scoreThreshold"]) \
                .setLinkThreshold(self.stage_config["imageTextDetectorCraft"]["linkThreshold"]) \
                .setWithRefiner(self.stage_config["imageTextDetectorCraft"]["refiner"]) \
                .setTextThreshold(self.stage_config["imageTextDetectorCraft"]["textThreshold"]) \
                .setSizeThreshold(self.stage_config["imageTextDetectorCraft"]["sizeThreshold"]) \
                .setUseGPU(self.use_gpu) \
                .setWidth(self.stage_config["imageTextDetectorCraft"]["width"]) \
                .setHeight(self.stage_config["imageTextDetectorCraft"]["height"])


            stages.append(text_detector)


            if self.ocr == "v2":
                ocr = ImageToTextV2.pretrained(self.stage_config["imageToTextV2"]["checkpoint"], "en", "clinical/ocr") \
                    .setRegionsColumn("text_regions") \
                    .setInputCols(["image_raw"]) \
                    .setOutputCol("text") \
                    .setOutputFormat("text_with_positions") \
                    .setGroupImages(self.stage_config["imageToTextV2"]["groupImages"]) \
                    .setKeepInput(True) \
                    .setUseGPU(self.use_gpu) \
                    .setUseCaching(True) \
                    .setBatchSize(self.stage_config["imageToTextV2"]["batchSize"]) \
                    .setTaskParallelism(self.stage_config["imageToTextV2"]["taskParallelism"])

                stages.append(ocr)

            else:
                ocr = ImageToTextV3() \
                    .setInputCols(["image_raw", "text_regions"]) \
                    .setOutputCol("text")

                stages.append(ocr)

        self.stages = stages + self.stages


    def post_nlp_stages(self):
        """
        Constructs post-NLP stages for deidentification/obfuscation.

        Raises:
            ValueError: If task is "obfuscate" but `keep_memory=False`.
        """

        logger.info("Building post-NLP stages...")

        stages = []

        if self.keep_memory:
            from sparknlp_jsl.annotator import DeIdentification

            sentence_or_document_input = self.document_assember_output_col if self.sentence_detector_output_col is None else self.sentence_detector_output_col

            deid_obfuscated = DeIdentification() \
                .setInputCols([sentence_or_document_input, self.regex_tokenizer_output_col, self.hc_output_col]) \
                .setOutputCol("obfuscated") \
                .setMode("obfuscate") \
                .setKeepMonth(self.stage_config["deidentification"]["keepMonth"]) \
                .setKeepYear(self.stage_config["deidentification"]["keepYear"]) \
                .setObfuscateDate(self.stage_config["deidentification"]["obfuscateDate"]) \
                .setSameEntityThreshold(self.stage_config["deidentification"]["sameEntityThreshold"]) \
                .setKeepTextSizeForObfuscation(self.stage_config["deidentification"]["keepTextSizeForObfuscation"]) \
                .setFakerLengthOffset(self.stage_config["deidentification"]["fakerLengthOffset"]) \
                .setReturnEntityMappings(True) \
                .setDays(self.stage_config["deidentification"]["days"]) \
                .setMappingsColumn("aux") \
                .setIgnoreRegex(self.stage_config["deidentification"]["ignoreRegex"]) \
                .setGroupByCol("path") \
                .setRegion(self.stage_config["deidentification"]["region"]) \
                .setSeed(self.stage_config["deidentification"]["seed"]) \
                .setConsistentObfuscation(True) \
                .setChunkMatching(self.get_matcher_entities())

            cleaner = NerOutputCleaner() \
                .setInputCol("aux") \
                .setOutputCol("new_aux") \
                .setOutputNerCol("positions_ner")

            position_finder = PositionFinder() \
                .setInputCols("positions_ner") \
                .setOutputCol("coordinates") \
                .setPageMatrixCol("positions") \
                .setSmoothCoordinates(self.stage_config["positionFinder"]["smoothCoordinates"])

            stages.append(deid_obfuscated)
            stages.append(cleaner)
            stages.append(position_finder)

        else:

            position_finder = PositionFinder() \
                .setInputCols(self.hc_output_col) \
                .setOutputCol("coordinates") \
                .setPageMatrixCol("positions") \
                .setSmoothCoordinates(self.stage_config["positionFinder"]["smoothCoordinates"])

            stages.append(position_finder)

        if self.task == "obfuscate" and self.keep_memory is False:
            raise ValueError(f"Keep Memory Should be enabled for obfuscate task.")

        elif self.task == "deid":

            draw_regions = ImageDrawRegions() \
                .setInputCol("image_raw") \
                .setInputRegionsCol("coordinates") \
                .setPatchImages(False) \
                .setRectColor(self.stage_config["imageDrawRegions"]["rectColor"]) \
                .setFilledRect(True) \
                .setOutputCol("image_with_regions")

        else:

            draw_regions = ImageDrawRegions() \
                .setInputCol("image_raw") \
                .setInputRegionsCol("coordinates") \
                .setInputChunkMappingCol("new_aux") \
                .setPatchImages(True) \
                .setRectColor(self.stage_config["imageDrawRegions"]["rectColor"]) \
                .setFontStyle(self.stage_config["imageDrawRegions"]["fontStyle"]) \
                .setFontName(self.stage_config["imageDrawRegions"]["fontName"]) \
                .setDynamicColor(self.stage_config["imageDrawRegions"]["dynamicColor"]) \
                .setPatchBackgroundColor(self.stage_config["imageDrawRegions"]["patchBackgroundColor"]) \
                .setPatchTextColor(self.stage_config["imageDrawRegions"]["patchTextColor"]) \
                .setBlackList(self.stage_config["imageDrawRegions"]["blackList"]) \
                .setRedactionEntityList(self.stage_config["imageDrawRegions"]["redactionList"]) \
                .setOutputCol("image_with_regions")

        stages.append(draw_regions)

        if self.input_type == "pdf" and self.pdf_finalizer:

            img_to_pdf = ImageToPdf() \
                .setPageNumCol("pagenum") \
                .setOriginCol("path") \
                .setOutputCol("pdf") \
                .setInputCol("image_with_regions") \
                .setAggregatePages(True)

            stages.append(img_to_pdf)

        self.stages = self.stages + stages


    def build(self):
        """
        Builds the full deidentification pipeline.

        Steps:
            1. Initialize empty stage list.
            2. Extract pretrained NLP stages (`nlp_builder`).
            3. Build OCR/pre-processing stages (`pre_nlp_stages`).
            4. Build post-processing stages (`post_nlp_stages`).
            5. Return assembled Spark ML `Pipeline`.

        Returns:
            pyspark.ml.Pipeline: Configured Spark ML pipeline object.
        """

        self.stages = []
        self.hc_output_col = None

        self.nlp_builder()
        self.pre_nlp_stages()
        self.post_nlp_stages()

        logger.info("Building full pdf deidentification pipeline...")

        model = Pipeline(stages=self.stages)

        return model