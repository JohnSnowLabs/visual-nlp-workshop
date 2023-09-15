FROM datamechanics/spark:3.2.1-dm17 
USER root
RUN apt-get update --allow-unauthenticated --allow-insecure-repositories && apt-get install nginx vim g++ -y --no-install-recommends && apt-get clean

RUN apt install python3-pip -y
RUN pip3 install --upgrade pip

# Run pipenv to install the dependencies
COPY ./requirements-dit.txt ./requirements-dit.txt
COPY ./requirements-new.txt ./requirements-new.txt
COPY ./requirements.txt ./requirements.txt

RUN pip3 install -r requirements-dit.txt
RUN pip3 install -r requirements-new.txt
RUN pip3 install -r requirements.txt

# Set and add the working directory in the container to /app

ENV PYTHONPATH "${PYTHONPATH}:.:./classification"
EXPOSE 8888
