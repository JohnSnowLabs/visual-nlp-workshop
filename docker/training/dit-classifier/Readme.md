## Fine Tuning Dit Based Visual NLP models
In this tutorial we walk you through setting the enviroment for fine-tuning Dit based classifiers using Docker containers.
### Make sure you have the correct hardware
Make sure your environment supports the following requirements,
* Storage: The image with all dependencies occupies ~ 12GB. Consider additional space for your datasets.
* GPU: Although CPU training is possible, 

### Start the container
sudo docker build . -t contimg-jsl

sudo docker run --gpus all -it -p 8020:8020 contimg-jsl

### Optional: port forwarding in AWS EC2
This section is not strictly required, but for those not having the required hardware, it could be useful to run this on an AWS instance.
This is how you connect to Jupyter running in your AWS instance,
```
ssh -i "key.pem" -L 9500:localhost:8888 ubuntu@aws-public-name

```

aws-public-name is something like ec2-5-176-17-99.us-east-1.compute.amazonaws.com.
