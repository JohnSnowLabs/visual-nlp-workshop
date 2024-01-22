# Run Visual NLP in docker container

1. Copy file with keys to the content folder::

    cp your_keys.json ./content/spark_ocr.json

2. Build docker image::

    docker build -t visualnlp .

3. Run docker container::

    docker run --publish=0.0.0.0:8888:8888 visualnlp

4. Copy url from the console output with the token and open it in browser.
