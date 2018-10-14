package com.myCompany.scalaExample

import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object ReadJsonToDS {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("ReadJsonToDS").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val ds = sqlContext.read.json("./input/multilinecolors.json").as[String]

    ds.printSchema()

    val df = ds.toDF()
    df.registerTempTable("JSONdata")

    val data=sqlContext.sql("select * from JSONdata")

    data.show()

    ds.rdd.foreach(x=> println(x))

    sc.stop
  }
}
