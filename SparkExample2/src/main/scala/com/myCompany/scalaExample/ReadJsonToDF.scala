package com.myCompany.scalaExample


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

object ReadJsonToDF {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ReadJsonToDF").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.json("./input/multilinecolors.json")

    df.printSchema()

    df.registerTempTable("JSONdata")

    val data=sqlContext.sql("select * from JSONdata")

    data.show()

    df.rdd.foreach(x=> println(x))

    sc.stop
  }
}
