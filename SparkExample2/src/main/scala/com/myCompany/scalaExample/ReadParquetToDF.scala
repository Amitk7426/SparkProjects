package com.myCompany.scalaExample

import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object ReadParquetToDF {
  case class textCase(date: String, A: Float, B:Float, C:Float, D:Float, E:Long, F:Float)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDDToParque").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val textDF = sqlContext.read.format("parquet").load("C:\\Users\\Amit\\Desktop\\TestSpark\\SparkExample2\\output\\parquet\\user.parquet")

    textDF.show()

    sc.stop
  }
}