### Running Visual NLP with GPU on Databricks
#### DB runtime 15.4 LTS ML (includes Apache Spark 3.5.0, GPU, Scala 2.12)
You need to install these dependencies,
```
com.johnsnowlabs.nlp:spark-nlp-gpu_2.12:5.5.2
Spark-NLP-JSL==5.5.2
Spark-OCR==5.5.1
com.microsoft.onnxruntime:onnxruntime_gpu:1.18.0
```
Make sure you exclude the following package when installing spark-nlp-gpu,

```
com.microsoft.onnxruntime:onnxruntime_gpu:1.19.0
```
The final config for spark-nlp-gpu must look something like this,




#### DB runtime 16.0
Just install the following packages,

```
com.johnsnowlabs.nlp:spark-nlp-gpu_2.12:5.5.2
Spark-NLP-JSL==5.5.2
Spark-OCR==5.5.1
com.microsoft.onnxruntime:onnxruntime_gpu:1.18.0
```
