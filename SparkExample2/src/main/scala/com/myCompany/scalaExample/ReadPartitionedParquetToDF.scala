package com.myCompany.scalaExample

import org.apache.spark.{SparkConf, SparkContext}

object ReadPartitionedParquetToDF {
  case class textCase(date: String, A: Float, B:Float, C:Float, D:Float, E:Long, F:Float)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ReadPartitionedParquetToDF").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val textDF = sqlContext.read.format("parquet").load("./input/parquet/partition/date=2004-01-07")

    textDF.show()

    val textDFDatePaceholder = sqlContext.read.format("parquet").load("./input/parquet/partition/date=2004-01-*")

    textDFDatePaceholder.show()

    val textDFMonthPaceholder = sqlContext.read.format("parquet").load("./input/parquet/partition/date=2004-*-03")

    textDFMonthPaceholder.show()

    val textDFYearPaceholder = sqlContext.read.format("parquet").load("./input/parquet/partition/date=*-01-03")

    textDFYearPaceholder.show()

  }
}
