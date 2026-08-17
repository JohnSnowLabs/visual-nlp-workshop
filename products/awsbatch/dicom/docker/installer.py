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

# dicom_deid_full_anonymization is JSL's officially maintained, fully
# pretrained DICOM de-id pipeline (OCR detection + clinical NER + metadata
# de-id, all bundled). It blanket-redacts every detected text region in the
# image instead of relying on a hand-assembled NER-chunk-to-pixel-position
# match (see products/awsbatch/dicom/docker/README.md for why: the more
# surgical hand-built pipelines from the reference notebooks left PHI
# visible whenever a NER chunk's text spanned more than one OCR-detected
# line/region, which PositionFinder then couldn't resolve to a bounding
# box).
from sparkocr.pretrained import PretrainedPipeline

pretrained = PretrainedPipeline("dicom_deid_full_anonymization", "en", "clinical/ocr")
pretrained.model.save("/opt/ml/dicom_pipeline")
shutil.rmtree("/app/model_cache")
