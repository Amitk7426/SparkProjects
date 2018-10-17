package com.myCompany.javaExample;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.myCompany.javaExample.Utils.Characters;

public class SparkMongoConnectDataFrame {

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

		Map<String, String> writeOverrides = new HashMap<String, String>();
		writeOverrides.put("collection", "myNewColl");
		WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);

		List<String> characters = Arrays.asList(
		    "{'name': 'Bilbo Baggins', 'age': 50}",
		    "{'name': 'Gandalf', 'age': 1000}",
		    "{'name': 'Thorin', 'age': 195}",
		    "{'name': 'Balin', 'age': 178}",
		    "{'name': 'Kíli', 'age': 77}",
		    "{'name': 'Dwalin', 'age': 169}",
		    "{'name': 'Óin', 'age': 167}",
		    "{'name': 'Glóin', 'age': 158}",
		    "{'name': 'Fíli', 'age': 82}",
		    "{'name': 'Bombur'}"
		);
		
		JavaRDD<Document> sparkDocuments = jsc.parallelize(characters).map(new Function<String, Document>() {
		    public Document call(final String json) throws Exception {
		        return Document.parse(json);
		    }
		});
		
		// Saving data from an RDD to MongoDB
		MongoSpark.save(sparkDocuments, writeConfig);
		
		Map<String, String> readOverrides = new HashMap<String, String>();
		readOverrides.put("collection", "myNewColl");
		readOverrides.put("readPreference.name", "secondaryPreferred");
		ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
		
		DataFrame df = MongoSpark.load(jsc, readConfig).toDF();
		df.printSchema();
		
		DataFrame explicitDF = MongoSpark.load(jsc, readConfig).toDF(Characters.class);
		explicitDF.printSchema();
		
		SQLContext sqlContext = SQLContext.getOrCreate(jsc.sc());
		explicitDF.registerTempTable("characters");
		DataFrame centenarians = sqlContext.sql("SELECT name, age FROM characters");
		
		MongoSpark.write(centenarians).option("collection", "hundredClub").mode("overwrite").save();
		
		MongoSpark.load(sqlContext, ReadConfig.create(sqlContext).withOption("collection", "hundredClub"), Characters.class).show();

		JavaMongoRDD<Document> javaMongoRDD = MongoSpark.load(jsc, ReadConfig.create(sqlContext).withOption("collection", "hundredClub"));
		
		JavaMongoRDD<Document> aggregatedRdd = javaMongoRDD.withPipeline(Arrays.asList(
												Document.parse("{ $match: { age : { $gt : 100 } } }")
												, Document.parse("{ $match: { age : { $lt : 170 } } }")
												));
		aggregatedRdd.toDF().show();
	}
}
