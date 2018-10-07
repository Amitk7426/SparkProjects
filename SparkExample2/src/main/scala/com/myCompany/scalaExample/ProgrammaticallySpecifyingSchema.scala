package com.myCompany.scalaExample

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ProgrammaticallySpecifyingSchema {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ProgrammaticallySpecifyingSchema").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val inputRDD = sc.textFile("./input/uber")
    val header = inputRDD.first()

    val schemaString = header

    import org.apache.spark.sql.Row;
    import org.apache.spark.sql.types.{StructType, StructField, StringType};

    val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val eliminate = inputRDD.filter(line => line != header)

    val rowRDD = eliminate.map(_.split(",")).map(e => Row(e(0), e(1), e(2), e(3)))

    val uberDF = sqlContext.createDataFrame(rowRDD, schema)

    uberDF.show()

  }
}
