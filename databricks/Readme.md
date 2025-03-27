### Running Visual NLP with GPU on Databricks
#### DB runtime 15.4 LTS ML (includes Apache Spark 3.5.0, GPU, Scala 2.12)
You need to install these dependencies,
```
maven:com.johnsnowlabs.nlp:spark-nlp-gpu_2.12:5.5.2
jar:spark-nlp-jsl==5.5.2
jar:spark-ocr==5.5.1
pypi:spark-ocr==5.5.1
maven:com.microsoft.onnxruntime:onnxruntime_gpu:1.18.0
```
Make sure you exclude the following package when installing spark-nlp-gpu,

```
maven:com.microsoft.onnxruntime:onnxruntime_gpu:1.19.0
```
The final config for spark-nlp-gpu must look like this,

![db_config](https://github.com/user-attachments/assets/c53f33a9-6eb5-4634-9907-074b35b94c87)



#### DB runtime 16.0
Just install the following packages,

```
maven:com.johnsnowlabs.nlp:spark-nlp-gpu_2.12:5.5.2
jar:spark-nlp-jsl==5.5.2
jar:spark-ocr==5.5.1
pypi:spark-ocr==5.5.1
```
