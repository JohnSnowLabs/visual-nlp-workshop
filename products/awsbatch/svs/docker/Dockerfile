FROM ubuntu:22.04
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV DEBIAN_FRONTEND=noninteractive
ENV HARDWARE_TARGET="cpu"
ENV TF_CPP_MIN_LOG_LEVEL='3'
ENV MODEL_TO_LOAD="image_deid_multi_model_context_pipeline_cpu"
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

COPY app.py installer.py /app/
RUN --mount=type=secret,id=license python3 /app/installer.py && rm -rf /root/.javacpp/* /tmp/*

RUN pip install --upgrade pillow

WORKDIR /app
ENV PATH=/opt/program:${PATH}
ENTRYPOINT ["python3", "app.py"]
