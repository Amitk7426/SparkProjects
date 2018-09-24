package com.myCompany.scalaExample

import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object RDDToJSON {
  case class textCase(date: String, A: Float, B:Float, C:Float, D:Float, E:Long, F:Float)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MaxPrice").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val textRDD = sc.textFile("C:\\Users\\Amit\\Desktop\\TestSpark\\SparkExample2\\input\\table.csv",1)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val textDF = textRDD.map(_.split(","))
      .map(p=> textCase(p(0), p(1).toFloat, p(2).toFloat, p(3).toFloat, p(4).toFloat, p(5).toLong, p(1).toFloat))
      .toDF()

    textDF.show()

    textDF.write.format("json").mode(SaveMode.Overwrite).save("C:\\Users\\Amit\\Desktop\\TestSpark\\SparkExample2\\output\\json")

    sc.stop
  }
}
