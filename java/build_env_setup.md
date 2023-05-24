## Steps for building with Java

There is possibility to use Spark-OCR with pure java. You may add local spark-ocr jar file to Maven's pom configuration file.

## Maven configuration

At first download spark-ocr jar file to local environment and place to your project.
Then add to pom.xml following part
```
      <dependency>
         <groupId>com.johnsnowlabs.nlp</groupId>
         <artifactId>spark-nlp-ocr</artifactId>
         <version>4.4.1</version>
         <scope>system</scope>
         <systemPath>{PATH_TO_spark-ocr-assembly-4.4.1.jar}</systemPath>
      </dependency>
      <dependency>
         <groupId>com.johnsnowlabs.nlp</groupId>
         <artifactId>spark-nlp_2.12</artifactId>
         <version>4.4.1</version>
      </dependency>
      <dependency>
         <groupId>org.scala-lang</groupId>
         <artifactId>scala-library</artifactId>
         <version>2.12.15</version>
      </dependency>
      <dependency>
         <groupId>org.apache.spark</groupId>
         <artifactId>spark-core_2.12</artifactId>
         <version>3.2.1</version>
      </dependency>
      <dependency>
         <groupId>io.netty</groupId>
         <artifactId>netty-all</artifactId>
         <version>4.1.68.Final</version>
      </dependency>
      <dependency>
         <groupId>org.bytedeco.javacpp-presets</groupId>
         <artifactId>tesseract-platform</artifactId>
         <version>4.0.0-1.4.4</version>
      </dependency>
      <dependency>
         <groupId>org.bytedeco</groupId>
         <artifactId>tesseract-platform</artifactId>
         <version>5.0.1-1.5.7</version>
      </dependency>
```
Please note that versions are given for the recentest 4.4.1 version at this moment. In the future you will need to update versions accordingly.
For spark-ocr 3.* and 4.* spark-core version should be 3.* You may try to use one which is already used or 3.2.1 as at this sample.
Compatibility matrix
| spark-ocr | spark-nlp | spark-core |
| --- | --- | --- |
| 4.4.1 | 4.4.1 | 3.2.1 |

## Apple M1 environment

For compiling for M1 envrionment you should substitute some of dependencies with one dedicated for that environment
```
      <dependency>
         <groupId>com.johnsnowlabs.nlp</groupId>
         <artifactId>spark-nlp-m1_2.12</artifactId>
         <version>4.4.1</version>
      </dependency>
      <dependency>
         <groupId>com.johnsnowlabs.nlp</groupId>
         <artifactId>tensorflow-m1_2.13</artifactId>
         <version>0.4.4</version>
      </dependency>
```

## Sample application

It is simple sample application just to check that you added dependencies correctly. If it is build and run OK then most probably everythig is set up correctly and you may start building your Spark OCR java application.
```
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.johnsnowlabs.ocr.transformers.PdfToHocr;

public class OcrSample {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			    .set("spark.executor.instances", "1")
			    .set("spark.sql.legacy.allowUntypedScalaUDF", "true")
			    .set("spark.kryoserializer.buffer.max", "200M");
		System.setProperty("jsl.settings.license",
				"{YOUR_LICENSE}");
		SparkSession session = SparkSession
				.builder()
				.config(
						sparkConf
						.setMaster("local[*]")
						.setAppName("OCR Sample")
						)
				.getOrCreate();
		PdfToHocr transformer = new PdfToHocr()
				.setInputCol("content")
				.setOutputCol("hocr");
		
		String pdfPath = "{PATH_TO_YOUR_PDF}";
		Dataset<Row> df = session.read().format("binaryFile").load(pdfPath);
		transformer.transform(df).select("hocr").collect();
		}
}
```
