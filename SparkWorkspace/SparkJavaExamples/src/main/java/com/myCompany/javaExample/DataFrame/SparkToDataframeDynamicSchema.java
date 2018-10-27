package com.myCompany.javaExample.DataFrame;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.myCompany.javaExample.Utils.Person;

public class SparkToDataframeDynamicSchema {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SparkToDataframe");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

		// Load a text file and convert each line to a JavaBean.
		JavaRDD<String> people = sc.textFile("examples/src/main/resources/people.txt");

		// The schema is encoded in a string
		String schemaString = "name age";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName: schemaString.split(" ")) {
		  fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);

		// Convert records of the RDD (people) to Rows.
		JavaRDD<Row> rowRDD = people.map(
		  new Function<String, Row>() {
		    public Row call(String record) throws Exception {
		      String[] fields = record.split(",");
		      return RowFactory.create(fields[0], fields[1].trim());
		    }
		  });

		// Apply the schema to the RDD.
		DataFrame peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema);

		// Register the DataFrame as a table.
		peopleDataFrame.registerTempTable("people");

		// SQL can be run over RDDs that have been registered as tables.
		DataFrame results = sqlContext.sql("SELECT name FROM people");

		// The results of SQL queries are DataFrames and support all the normal RDD operations.
		// The columns of a row in the result can be accessed by ordinal.
		List<String> peoplesNames = results.javaRDD().map(new Function<Row, String>() {
		  public String call(Row row) {
		    return "Name: " + row.getString(0);
		  }
		}).collect();
		
		peoplesNames.forEach(x->System.out.println(x));
	}

}
