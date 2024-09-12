package com.johnsnowlabs.ocr.samples.java;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.johnsnowlabs.ocr.ImageType;
import com.johnsnowlabs.ocr.transformers.BinaryToImage;
import com.johnsnowlabs.ocr.transformers.ImageTextCleaner;
import com.johnsnowlabs.ocr.transformers.ImageToText;

public class ImageTextCleanerSample {
	
	public static PipelineModel getTextCleaningPipelineModel(SparkSession session) {
	    PipelineModel pipelineModel = null;
	    try {
	    	BinaryToImage binaryToImage = new BinaryToImage();
	        binaryToImage.setInputCol("content");
	        binaryToImage.setOutputCol("image");
	        binaryToImage.setImageType(ImageType.TYPE_3BYTE_BGR());

	        ImageTextCleaner textCleaner = (ImageTextCleaner) ImageTextCleaner
	        		.pretrained("text_cleaner_v1", "en", "clinical/ocr");
	        textCleaner.setInputCol("image");
	        textCleaner.setOutputCol("cleaned_image");
	        textCleaner.setSizeThreshold(5);
	        textCleaner.setTextThreshold(0.4);
	        textCleaner.setLinkThreshold(0.4);
	        textCleaner.setPadding(10);
	        textCleaner.setMedianBlur(3);
	        textCleaner.setBinarize(true);
	        textCleaner.setWidth(960);
	        textCleaner.setHeight(1280);

	        ImageToText ocr = new ImageToText();
	        ocr.setInputCol("cleaned_image");
	        ocr.setOutputCol("text");
	        ocr.setIgnoreResolution(false);

	        Pipeline pipeline = new Pipeline();
	        pipeline.setStages(new PipelineStage[] {
	        		binaryToImage,
	        		textCleaner,
	        		ocr
	        		});

	        pipelineModel = pipeline.fit(session.emptyDataFrame());
	        return pipelineModel;

	    } catch (Exception e) {
	        e.printStackTrace();
	        System.out.println(e.getLocalizedMessage());
	    }
	    return pipelineModel;
	}

	public static void main(String[] args) {
		SparkSession session = SessionBuilder.getSparkSession();
		String imgPath = ImageTextCleanerSample
				.class
				.getClassLoader()
				.getResource("020_Yas_patella.jpg")
				.getPath();
		Dataset<Row> df = session.read().format("binaryFile").load(imgPath);
		getTextCleaningPipelineModel(session)
		.transform(df)
		.show();
	}

}
