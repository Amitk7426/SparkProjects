package com.myCompany.javaExample;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import com.myCompany.javaExample.Utils.Person;

public class SparkHiveConnect {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SparkHiveConnect");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(sc);

		JavaRDD<Person> peopleRDD = sc.textFile("./input/person.txt").map(new Function<String, Person>() {
			public Person call(String line) throws Exception {
				String[] parts = line.split(",");

				Person person = new Person();
				person.setFirstName(parts[0]);
				person.setLastName(parts[1]);
				person.setGender(parts[2]);
				return person;
			}
		});

		DataFrame personDataFrame = hiveContext.createDataFrame(peopleRDD, Person.class);
		
		personDataFrame.show();
		
		
		personDataFrame.write().format("orc").mode(SaveMode.Overwrite)
				.option("path", "hdfs://0.0.0.0:9000/user/hive/warehouse/")
				.saveAsTable("PersonJava");
	}
}
