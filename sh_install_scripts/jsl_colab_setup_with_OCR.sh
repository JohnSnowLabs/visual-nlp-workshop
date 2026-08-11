#!/bin/sh

JSON_FILE="${1:-license.json}"

if [ -f "$JSON_FILE" ]; then
  echo "Reading License file from $JSON_FILE"

  export PUBLIC_VERSION=$(jq -r '.PUBLIC_VERSION // empty' "$JSON_FILE")
  export SPARK_OCR_SECRET=$(jq -r '.SPARK_OCR_SECRET // empty' "$JSON_FILE")
  export OCR_VERSION=$(jq -r '.OCR_VERSION // empty' "$JSON_FILE")
  export SECRET=$(jq -r '.SECRET // empty' "$JSON_FILE")
  export JSL_VERSION=$(jq -r '.JSL_VERSION // empty' "$JSON_FILE")
  export AWS_ACCESS_KEY_ID=$(jq -r '.AWS_ACCESS_KEY_ID // empty' "$JSON_FILE")
  export AWS_SECRET_ACCESS_KEY=$(jq -r '.AWS_SECRET_ACCESS_KEY // empty' "$JSON_FILE")
  export AWS_SESSION_TOKEN=$(jq -r '.AWS_SESSION_TOKEN // empty' "$JSON_FILE")

  echo "License file loaded"
else
  echo "JSON file not found: $JSON_FILE"
fi

echo "Installing Visual NLP ( SPARK-OCR ) - $OCR_VERSION"
pip install -q --force-reinstall spark-ocr==$OCR_VERSION --user --extra-index-url=https://pypi.johnsnowlabs.com/$SPARK_OCR_SECRET

echo "Installing Healthcare NLP - $JSL_VERSION"
pip install -q --force-reinstall spark_nlp_jsl==$JSL_VERSION --user --extra-index-url=https://pypi.johnsnowlabs.com/$SECRET

echo "Installing Spark NLP - $PUBLIC_VERSION"
pip install -q spark-nlp==$PUBLIC_VERSION

echo "Installing Java 8"
apt-get update
apt-get remove -y openjdk-11-jdk openjdk-11-jre openjdk-17-jdk openjdk-17-jre
apt-get autoremove -y
apt-get update -qq
apt-get install -y openjdk-8-jdk
