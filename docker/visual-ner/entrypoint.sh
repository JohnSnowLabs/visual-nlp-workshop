#!/bin/bash

export_json () {
    for s in $(echo $values | jq -r 'to_entries|map("\(.key)=\(.value|tostring)")|.[]' $1 ); do
        export $s
    done
}

export_json "/content/visualnlp.json"

pip install --upgrade spark-ocr==$OCR_VERSION --user --extra-index-url https://pypi.johnsnowlabs.com/$SPARK_OCR_SECRET

if [ $? != 0 ];
then
    exit 1
fi

jupyter lab --ip 0.0.0.0 --port 8888 --no-browser --allow-root
