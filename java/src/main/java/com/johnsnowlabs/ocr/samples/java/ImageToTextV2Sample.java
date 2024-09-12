package com.johnsnowlabs.ocr.samples.java;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.johnsnowlabs.ocr.ImageType;
import com.johnsnowlabs.ocr.transformers.BinaryToImage;
import com.johnsnowlabs.ocr.transformers.ImageTextDetector;
import com.johnsnowlabs.ocr.transformers.recognizers.ImageToTextV2;

import sparkocr.transformers.detectors.ImageTextDetectorV2;

public class ImageToTextV2Sample {
	
	public static PipelineModel getHandwrittenPipelineModel(SparkSession session) {
	    PipelineModel pipelineModel = null;
	    try {
	    	BinaryToImage binaryToImage = new BinaryToImage();
	        binaryToImage.setInputCol("content");
	        binaryToImage.setOutputCol("image");
	        binaryToImage.setImageType(ImageType.TYPE_3BYTE_BGR());

	        ImageTextDetector textDetector = 
	        		(ImageTextDetector) ImageTextDetector
	        		.pretrained("text_detection_v1", "en", "clinical/ocr");
	        textDetector.setInputCol("image");
	        textDetector.setOutputCol("text_regions");
	        textDetector.setSizeThreshold(10);
	        textDetector.setScoreThreshold(0.9);
	        textDetector.setLinkThreshold(0.4);
	        textDetector.setTextThreshold(0.2);

//	        ImageTextDetectorV2 textDetector = 
//	        		(ImageTextDetectorV2) ImageTextDetectorV2
//	        		.pretrained("image_text_detector_v2", "en", "clinical/ocr");
//	        textDetector.setInputCol("image") ;
//	        textDetector.setOutputCol("text_regions") ;
//	        textDetector.setSizeThreshold(-1) ;
//	        textDetector.setLinkThreshold(0.3);
//	        textDetector.setWidth(500);
//	        textDetector.setWithRefiner(true);

	        ImageToTextV2 imageToTexV2t = 
	        		ImageToTextV2
	        		.pretrained("ocr_base_handwritten_v2_opt", "en", "clinical/ocr");
	        imageToTexV2t.setInputCols(new String[]{"image", "text_regions"}) ;
	        imageToTexV2t.setOutputCol("text");
	        imageToTexV2t.setGroupImages(true);
	        imageToTexV2t.setRegionsColumn("text_regions");

	        Pipeline pipeline = new Pipeline();
	        pipeline.setStages(new PipelineStage[] {
	        		binaryToImage,
	        		textDetector,
	        		imageToTexV2t
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
		String imgPath = ImageToTextV2Sample
				.class
				.getClassLoader()
				.getResource("handwritten_example.jpg")
				.getPath();
		Dataset<Row> df = session.read().format("binaryFile").load(imgPath);
		getHandwrittenPipelineModel(session)
		.transform(df)
		.show();
	}

}
