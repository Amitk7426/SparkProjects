package com.myCompany.scalaExample

/**
 * Hello world!
 *
 */
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf,SparkContext}

object App {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MaxPrice")
    val sc = new SparkContext(conf)

    println( "Hello World!" )

    sc.textFile(args(0))
      .map(_.split(","))
      .map(rec => ((rec(0).split("-"))(0).toInt, rec(1).toFloat))
      .reduceByKey((a,b) => Math.max(a,b))
      .saveAsTextFile(args(1))
  }
}
