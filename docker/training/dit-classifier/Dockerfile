FROM python:3.10

RUN apt-get update
RUN apt-get update && apt-get install nginx vim g++ -y --no-install-recommends && apt-get clean
RUN ln -sf /dev/stdout /var/log/nginx/access.log \
    && ln -sf /dev/stderr /var/log/nginx/error.log


# Run pipenv to install the dependencies
COPY ./requirements-dit.txt ./requirements-dit.txt
COPY ./requirements-new.txt ./requirements-new.txt
COPY ./requirements.txt ./requirements.txt
RUN pip install torch
RUN pip install -r requirements-dit.txt
RUN pip install -r requirements-new.txt
RUN pip install -r requirements.txt



RUN pip uninstall -y protobuf
RUN pip install protobuf==3.20
RUN pip uninstall -y deepspeed
RUN pip install deepspeed



# Set and add the working directory in the container to /app
WORKDIR /app
COPY ./ /app/

ENV PYTHONPATH "${PYTHONPATH}:.:./classification"

# Execute the application
RUN chown -R www-data:www-data /app/
EXPOSE 8020
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8020"]