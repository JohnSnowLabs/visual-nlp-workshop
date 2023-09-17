## Fine Tuning Dit Based Visual NLP models
In this tutorial we walk you through setting the enviroment for fine-tuning Dit based classifiers using Docker containers.
### Make sure you have the correct hardware
Make sure your environment supports the following requirements,
* Storage: The image with all dependencies occupies ~ 12GB. Consider additional space for your datasets.
* GPU: Although CPU training is possible, we recommend you use a GPU.

If you don't have the hardware, please jump to the AWS section for details on how to do the setup on AWS.

### Build Image & start the container
Start by clonning the repo and building the image,
```
git clone https://github.com/JohnSnowLabs/spark-ocr-workshop.git
cd spark-ocr-workshop/docker/training/dit-classifier
sudo docker build . -t contimg-jsl
```
Run the container like this,
```
sudo docker run --gpus all -it --rm -p 8888:8888  -v "${PWD}":"/dit-classifier" -v "${PWD}/rvl-test":"/rvl-test" -v "${PWD}/rvl-train":"/rvl-train" contimg-jsl:latest bash
```
notice the mapping of local folders rvl-train, and rvl-test to corresponding container folders. You can change these paths to others of your convinience, or don't pass them if you are acessing datasets through some distributed file system like S3. More on this in the sample notebook.

```
# now inside the container run this,
export SPARK_OCR_LICENSE=...
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
jupyter notebook --allow-root --ip=0.0.0.0
```

### Optional: Setting up an EC2 instance on AWS
This section is not strictly required, but for those not having the required hardware, it could be useful to run this on an AWS instance. To run this example we used a g5.2xlarge instance type.

This is how you connect to Jupyter running in your AWS instance,
```
ssh -i "key.pem" -L 9500:localhost:8888 ubuntu@aws-public-name(*)

```

This will allow you to access Jupyer notebooks running in the instance from your local environment by entering localhost:9500 into your browser. If you are prompted for a password, check the Jupyter logs, and copy/paste the value into your browser.

(*)aws-public-name is something like ec2-5-176-14-99.us-east-3.compute.amazonaws.com.

### Run the fine-tuning notebook
At this point you must be ready to jump into the [TrainDitRvlCdip.ipynb](https://github.com/JohnSnowLabs/spark-ocr-workshop/blob/master/docker/training/dit-classifier/TrainDitRvlCdip.ipynb) notebook.

