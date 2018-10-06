package com.myCompany.scalaExample

import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object ReadParquetToDF {
  case class textCase(date: String, A: Float, B:Float, C:Float, D:Float, E:Long, F:Float)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ReadParquetToDF").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val textFormatDF = sqlContext.read.format("parquet").load("./input/parquet/user.parquet")
    textFormatDF.show()

    val textDF = sqlContext.read.parquet("./input/parquet/user.parquet")
    textDF.show()

    val df = sqlContext.sql("SELECT * FROM parquet. `./input/parquet/user.parquet`")

    df.show()
    sc.stop
  }
}
