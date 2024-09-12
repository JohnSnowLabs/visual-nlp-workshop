package com.johnsnowlabs.ocr.samples.java;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.johnsnowlabs.ocr.transformers.ImageSkewCorrector;
import com.johnsnowlabs.ocr.transformers.ImageToText;
import com.johnsnowlabs.ocr.transformers.PdfToImage;

public class SkewCorrectionSample {
	
	public static PipelineModel getSkewCorrectionPipelineModel(
			Dataset<Row> df
			) {
	    PipelineModel pipelineModel = null;
	    try {
	    	PdfToImage pdfToImage = new PdfToImage();
	    	pdfToImage.setInputCol("content");
	    	pdfToImage.setOutputCol("image");
	        
	        ImageSkewCorrector skewCorrector = new ImageSkewCorrector();
	        skewCorrector.setInputCol("image");
	        skewCorrector.setOutputCol("corrected_image");
	        skewCorrector.setAutomaticSkewCorrection(true);

	        ImageToText ocr = new ImageToText();
	        ocr.setInputCol("image");
	        ocr.setOutputCol("text");

	        Pipeline pipeline = new Pipeline();
	        pipeline.setStages(new PipelineStage[] {
	        		pdfToImage,
	        		skewCorrector,
	        		ocr
	        		});

	        pipelineModel = pipeline.fit(df);
	        return pipelineModel;

	    } catch (Exception e) {
	        e.printStackTrace();
	        System.out.println(e.getLocalizedMessage());
	    }
	    return pipelineModel;
	}

	public static void main(String[] args) {
		SparkSession session = SessionBuilder.getSparkSession();
		String imgPath = SkewCorrectionSample
				.class
				.getClassLoader()
				.getResource("400_rot.pdf")
				.getPath();
		Dataset<Row> df = session.read().format("binaryFile").load(imgPath);
		df = getSkewCorrectionPipelineModel(df)
		.transform(df);
		System.out.println(df.first().getAs("text").toString());
	}

}
