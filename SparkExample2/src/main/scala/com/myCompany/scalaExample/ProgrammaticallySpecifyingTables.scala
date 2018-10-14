package com.myCompany.scalaExample

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object ProgrammaticallySpecifyingTables {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ProgrammaticallySpecifyingTables").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val inputRDD = sc.textFile("./input/testSql.txt")
    val header = inputRDD.flatMap(_.split(";"))

    //header.collect().foreach(x=> x.foreach({y=> y.split(" ").foreach(z=>println(z))}))

    //header.collect().foreach(x=> x.foreach({(z=>println(z))}))


    println(header.collect().foreach(z=>z.replace(",", " ")))
  }
}
