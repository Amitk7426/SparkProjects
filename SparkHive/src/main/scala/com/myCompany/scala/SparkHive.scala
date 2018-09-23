package com.myCompany.scala

import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkConf, SparkContext }

object SparkHive {
case class Person(firstName: String, lastName: String, gender: String)
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkHive").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    import hc.implicits._
    
    val personResult = sc.textFile("C:/Users/dell/Desktop/HadoopTest/testData/person.txt")
    val personRDD = personResult.map(_.split(",")).map(p => Person(p(0), p(1), p(2)))
    val person = personRDD.toDF
    person.show();
    person.registerTempTable("person1")
    val males = hc.sql("select * from person1")
    males.map(t => "Name: " + t(0)).collect().foreach(println)
	
    val options = Map("path" -> "hdfs://0.0.0.0:9000/user/hive/warehouse/")

    person.write.format("orc").options(options).mode("overwrite").saveAsTable("person_orc_New")
    sc.stop()
  }
}