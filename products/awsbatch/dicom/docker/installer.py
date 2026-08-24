import os
import shutil
from johnsnowlabs import nlp


HARDWARE_TARGET = os.environ.get("HARDWARE_TARGET", "cpu")

nlp.install(
    json_license_path="/run/secrets/license",
    browser_login=False,
    force_browser=False,
    hardware_platform=HARDWARE_TARGET,
    visual=True,
    refresh_install=True,
)

spark = nlp.start(model_cache_folder="/app/model_cache", visual=True)
spark.sparkContext.setLogLevel("ERROR")

# Pretrained end-to-end DICOM de-id pipeline: OCR detection, clinical NER,
# and metadata de-id, bundled together.
from sparkocr.pretrained import PretrainedPipeline

pretrained = PretrainedPipeline("dicom_deid_full_anonymization", "en", "clinical/ocr")
pretrained.model.save("/opt/ml/dicom_pipeline")
shutil.rmtree("/app/model_cache")
