package com.myCompany.javaExample.DataFrame;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.myCompany.javaExample.Utils.Person;

import org.apache.spark.api.java.function.Function;
// Import factory methods provided by DataTypes.
import org.apache.spark.sql.types.DataTypes;
// Import StructType and StructField
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
// Import Row.
import org.apache.spark.sql.Row;
// Import RowFactory.
import org.apache.spark.sql.RowFactory;

public class SparkToDataframe {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SparkToDataframe");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

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

		DataFrame schemaPeople = sqlContext.createDataFrame(peopleRDD, Person.class);
		schemaPeople.registerTempTable("people");

		DataFrame peoples = sqlContext.sql("SELECT * FROM people");
		
		List<String> peoplesNames = peoples.javaRDD().map(new Function<Row, String>() {
			  public String call(Row row) {
			    return "firstName: " + row.getString(0);
			  }
			}).collect();
		
		peoplesNames.forEach(x->System.out.println(x));
	}
}
