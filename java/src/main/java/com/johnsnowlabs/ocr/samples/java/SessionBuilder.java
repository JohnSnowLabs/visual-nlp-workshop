package com.johnsnowlabs.ocr.samples.java;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SessionBuilder {

	public static String OCR_LICENSE = ""; 

	public static SparkSession getSparkSession() {
		SparkConf sparkConf = new SparkConf()
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			    .set("spark.executor.instances", "1")
			    .set("spark.sql.legacy.allowUntypedScalaUDF", "true")
			    .set("spark.kryoserializer.buffer.max", "200M");
		System.setProperty("jsl.settings.license",
				OCR_LICENSE);
		SparkSession session = SparkSession
				.builder()
				.config(
						sparkConf
						.setMaster("local[*]")
						.setAppName("OCR Sample")
						)
				.getOrCreate();
		return session;
	}

}
