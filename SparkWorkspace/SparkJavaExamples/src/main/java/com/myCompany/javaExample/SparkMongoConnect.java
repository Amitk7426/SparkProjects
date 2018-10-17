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

public class SparkMongoConnect {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SparkMongoConnect")
								.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/testMongodb.movie")
								.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/testMongodb.movie");

		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		// Loading and analyzing data from MongoDB
		JavaRDD<Document> rdd = MongoSpark.load(jsc);
		System.out.println(rdd.count());
		System.out.println(rdd.first().toJson());

		JavaRDD<Document> sparkDocuments = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
		    (new Function<Integer, Document>() {
		        public Document call(final Integer i) throws Exception {
		            return Document.parse("{spark: " + i + "}");
		        }
		    });

		// Saving data from an RDD to MongoDB
		MongoSpark.save(sparkDocuments);
	}
}
