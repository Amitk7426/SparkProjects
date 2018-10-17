package com.myCompany.javaExample;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;

public class SparkMongoConnectConfig {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SparkMongoConnectConfig");

		sparkConf.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/");
		sparkConf.set("spark.mongodb.input.database", "testMongodb");
		sparkConf.set("spark.mongodb.input.collection", "movie");
		sparkConf.set("spark.mongodb.input.readPreference.name", "secondaryPreferred");
		
		sparkConf.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/");
		sparkConf.set("spark.mongodb.output.database", "testMongodb");
		sparkConf.set("spark.mongodb.output.collection", "movie");
		sparkConf.set("spark.mongodb.output.writeConcern.w", "majority");

		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		Map<String, String> readOverrides = new HashMap<String, String>();

		/*
		 * Can override collection name with readOverrides/writeOverrides parameter
		 * readOverrides.put("collection", "spark");
		 * readOverrides.put("readPreference.name", "secondaryPreferred");
		 */
		ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

		JavaRDD<Document> customRdd = MongoSpark.load(jsc, readConfig);

		System.out.println(customRdd.count());
		System.out.println(customRdd.first().toJson());

		Map<String, String> writeOverrides = new HashMap<String, String>();
		writeOverrides.put("collection", "spark");
		writeOverrides.put("writeConcern.w", "majority");
		WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);

		JavaRDD<Document> sparkDocuments = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
				.map(new Function<Integer, Document>() {
					public Document call(final Integer i) throws Exception {
						return Document.parse("{spark: " + i + "}");
					}
				});

		// Saving data from an RDD to MongoDB
		MongoSpark.save(sparkDocuments, writeConfig);
	}
}
