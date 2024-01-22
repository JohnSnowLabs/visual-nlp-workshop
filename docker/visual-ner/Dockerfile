FROM ubuntu:22.04

RUN apt-get -y update \
    && apt-get install -y wget \
    && apt-get install -y jq \
    && apt-get install -y lsb-release \
    && apt-get install -y openjdk-8-jdk-headless \
    && apt-get install -y build-essential python3-pip \
    && pip3 -q install pip --upgrade \
    && pip3 -q install jupyterlab \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
         /usr/share/man /usr/share/doc /usr/share/doc-base

ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYSPARK_PYTHON=python3

EXPOSE 8888

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

COPY entrypoint.sh /
RUN chmod +x /entrypoint.sh

COPY ./content/ /content/
WORKDIR content/

ENTRYPOINT ["/entrypoint.sh"]