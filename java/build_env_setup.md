## Steps for building with Java

## Maven configuration
```
      <dependency>
         <groupId>com.johnsnowlabs.nlp</groupId>
         <artifactId>spark-nlp-ocr</artifactId>
         <version>4.4.1</version>
         <scope>system</scope>
         <systemPath>{PATH_TO_spark-ocr-assembly-4.4.1.jar}</systemPath>
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
```

## Sample application
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
