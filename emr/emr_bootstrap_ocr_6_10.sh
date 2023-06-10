#!/bin/bash
set -x -e

sudo pip3 install --upgrade pip setuptools
sudo pip3 install pillow==9.0.1 imageio==2.16.0 pip ipython pandas spark-nlp==4.4.2
sudo pip3 install spark-ocr==4.4.2 --extra-index-url https://pypi.johnsnowlabs.com/4.4.2-bf3d5a91e8115514cd40cb8e424ad1bffbc3743d

sudo wget https://pypi.johnsnowlabs.com/4.4.2-bf3d5a91e8115514cd40cb8e424ad1bffbc3743d/jars/spark-ocr-assembly-4.4.2.jar -P /usr/lib/spark/jars
sudo wget https://s3.amazonaws.com/auxdata.johnsnowlabs.com/public/jars/spark-nlp-assembly-4.4.2.jar -P /usr/lib/spark/jars

set +x
exit 0
