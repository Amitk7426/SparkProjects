package com.myCompany.scalaExample

import org.apache.spark.{SparkConf, SparkContext}

object RDDtoDataframe {

  // 2016-03-31,12.25,12.70,12.15,12.60,1436400,12.60
  case class textCase(date: String, A: Float, B:Float, C:Float, D:Float, E:Long, F:Float)
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDDtoDataframe")
    val sc = new SparkContext(conf)

    val textRDD = sc.textFile(args(0))

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val textDF = textRDD.map(_.split(","))
                  .map(p=> textCase(p(0), p(1).toFloat, p(2).toFloat, p(3).toFloat, p(4).toFloat, p(5).toLong, p(1).toFloat))
                    .toDF()

    textDF.show()
  }
}
