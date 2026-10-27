import os
import shutil
from johnsnowlabs import nlp


HARDWARE_TARGET = os.environ.get("HARDWARE_TARGET", "cpu")
model_ref = os.environ.get("MODEL_TO_LOAD", None)

nlp.settings.enforce_versions = False

nlp.install(
    json_license_path="/run/secrets/license",
    browser_login=False,
    force_browser=False,
    hardware_platform=HARDWARE_TARGET,
    visual=True,
    refresh_install=True,
)

spark = nlp.start(model_cache_folder="/app/model_cache", visual=True)
from sparkocr.transformers import *
spark.sparkContext.setLogLevel("ERROR")

detector = ImageTextDetectorCraft.pretrained("image_text_detector_mem_opt",  "en", "clinical/ocr")
detector.save("/opt/ml/image_text_detector_mem_opt")

# the de-identification pipeline is only needed for DEID_MODE=pipeline
if model_ref:

    import sparkocr.internal as ocr_internal

    # only downloaded and unzipped: app.py loads it, loading it here needs a lot of memory
    path = ocr_internal._DownloadPipelineWithoutLoading(model_ref, "en", "clinical/ocr").apply()
    shutil.move(path[len("file:"):] if path.startswith("file:") else path, "/opt/ml/model")

shutil.rmtree("/app/model_cache")
