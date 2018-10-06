package com.myCompany.scalaExample

import org.apache.spark.{SparkConf, SparkContext}

object SortKeyOperation {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SortKeyOperation").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val inputRDD = sc.textFile("./input/uber")
    val header = inputRDD.first()
    val format = new java.text.SimpleDateFormat("MM/dd/yyyy")
    val days =Array("Sun","Mon","Tue","Wed","Thu","Fri","Sat")
    val eliminate = inputRDD.filter(line => line != header)
    val split = eliminate.map(line => line.split(",")).map { x => (x(0),format.parse(x(1)),x(3)) }
    val combines = split.map(x => (x._1+" "+days(x._2.getDay),x._3.toInt))
    combines.reduceByKey(_+_).map(item => item.swap).sortByKey(false).collect.foreach(println)

    println("""++++++++++++++++++++++++++++++++++++++++++++++++++++++""")
    combines.reduceByKey(_+_).map(item => item.swap).sortByKey(true).collect.foreach(println)

    //for practice
    val combine = split.map(x => (x._1+" "+x._2.getDay,x._3.toInt))
    combine.collect
  }
}
