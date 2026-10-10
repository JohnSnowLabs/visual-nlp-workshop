FROM ubuntu:22.04
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV DEBIAN_FRONTEND=noninteractive
# cpu or gpu: --build-arg HARDWARE_TARGET=gpu installs the GPU jars and model
ARG HARDWARE_TARGET=cpu
ENV HARDWARE_TARGET=${HARDWARE_TARGET}
ENV TF_CPP_MIN_LOG_LEVEL='3'
ENV MODEL_TO_LOAD="image_deid_multi_model_context_line_pipeline_${HARDWARE_TARGET}"
ENV CREATE_NEW_SVS_FILE="false"
# blanket: redact all detected text; pipeline: only the PHI found by MODEL_TO_LOAD
ENV DEID_MODE="pipeline"
# fewer glibc malloc arenas: keeps the JVM's native memory from growing across files
ENV MALLOC_ARENA_MAX=2
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/" >> ~/.bashrc
ENV LC_ALL=C.UTF-8 LANG=C.UTF-8


RUN apt-get update && apt-get install --no-install-recommends -y \
         python3 \
         python3-dev \
         python3-pip \
         python3-gdcm \
         libgdcm-tools \
         libgl1 \
         wget \
         openjdk-17-jdk-headless \
         git-core \
         curl && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
         && mkdir -p /app/model_cache


# Install Johnsnowlabs libraries
RUN pip config set global.no-cache-dir 1 && pip install packaging johnsnowlabs==6.4.1 fastapi torchvision==0.13.1 pillow_jpls pylibjpeg[all] pydicom pyjpegls pylibjpeg-openjpeg pylibjpeg highdicom boto3 imagecodecs

# Spark NLP version required by Visual NLP 7.0.1
ARG SPARK_NLP_VERSION=7.0.0
RUN sed -i "s/^raw_version_nlp = .*/raw_version_nlp = \"${SPARK_NLP_VERSION}\"/" \
    $(python3 -c "import importlib.util; print(importlib.util.find_spec('johnsnowlabs').submodule_search_locations[0])")/settings.py

# GPU: the CUDA and cuDNN versions ONNX Runtime is built with
RUN if [ "$HARDWARE_TARGET" = "gpu" ]; then pip install --target /opt/cuda-libs nvidia-cuda-runtime-cu12==12.8.90 \
        nvidia-cublas-cu12==12.8.5.5 nvidia-curand-cu12==10.3.10.19 nvidia-cufft-cu12==11.3.3.83 nvidia-cudnn-cu12==9.8.0.87; fi
ENV LD_LIBRARY_PATH=/opt/cuda-libs/nvidia/cuda_runtime/lib:/opt/cuda-libs/nvidia/cublas/lib:/opt/cuda-libs/nvidia/curand/lib:/opt/cuda-libs/nvidia/cufft/lib:/opt/cuda-libs/nvidia/cudnn/lib

COPY app.py installer.py /app/
RUN --mount=type=secret,id=license python3 /app/installer.py && rm -rf /root/.javacpp/* /tmp/*

RUN pip install --upgrade pillow

WORKDIR /app
ENV PATH=/opt/program:${PATH}
ENTRYPOINT ["python3", "app.py"]
