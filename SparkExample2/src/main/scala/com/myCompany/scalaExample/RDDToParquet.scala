package com.myCompany.scalaExample

import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object RDDToParquet {
  case class textCase(date: String, A: Float, B:Float, C:Float, D:Float, E:Long, F:Float)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDDToParque").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val textRDD = sc.textFile("./input/table.csv",1)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val textDF = textRDD.map(_.split(","))
      .map(p=> textCase(p(0), p(1).toFloat, p(2).toFloat, p(3).toFloat, p(4).toFloat, p(5).toLong, p(1).toFloat))
      .toDF()

    textDF.show()

    textDF.write.format("parquet").mode(SaveMode.Overwrite).save("./output/parquet/userFormat.parquet")

    textDF.write.parquet("./output/parquet/user.parquet")

    sc.stop
  }
}
