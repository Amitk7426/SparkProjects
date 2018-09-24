package com.myCompany.scalaExample

import org.apache.spark.{SparkConf, SparkContext}

object KeyOperations {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("KeyOperations").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val inputRDD = sc.textFile("C:\\Users\\Amit\\Desktop\\TestSpark\\SparkExample2\\input\\Police_Department_Incidents.csv")

    println("++++++++++++ReduceByKey+++++++++++++++")
    val top5CateogriesOfIncidentsReduceByKey = inputRDD.map(_.split(","))
                                                .map(x=>(x(1),1)).reduceByKey((x,y)=>x+y)
                                                .takeOrdered(5)(Ordering[Int].reverse.on(x=>x._2))

    top5CateogriesOfIncidentsReduceByKey.foreach(println)

    println("++++++++++++GroupByKey+++++++++++++++")
    val top5CateogriesOfIncidentsGroupByKey = inputRDD.map(_.split(","))
                                                .map(x=>(x(1),1)).groupByKey.map(x=>(x._1,x._2.sum))
                                                .takeOrdered(5)(Ordering[Int].reverse.on(x=>x._2))

    top5CateogriesOfIncidentsGroupByKey.foreach(println)

    println("+++++++++++++CombineByKey++++++++++++++")
    val top5CateogriesOfIncidentsCombineByKey = inputRDD.map(_.split(",")).map(x=>(x(1),1))
                                                .combineByKey(value => (value + value),
                                                  (intraPartitionAcc: Int, v) => (intraPartitionAcc + v),
                                                  (interPartitionAcc1: Int, interPartitionAcc2: Int) => (interPartitionAcc1 + interPartitionAcc2))
                                                  .takeOrdered(5)(Ordering[Int].reverse.on(x=>x._2))

    top5CateogriesOfIncidentsCombineByKey.foreach(println)

    println("+++++++++++++AggregateByKey++++++++++++++")
    val top5CateogriesOfIncidentsAggregateByKey = inputRDD.map(_.split(",")).map(x=>(x(1),1))
                                                  .aggregateByKey(0)(((x,y)=>x+y),((x,y)=>x+y))
                                                  .takeOrdered(5)(Ordering[Int].reverse.on(x=>x._2))
    top5CateogriesOfIncidentsAggregateByKey.foreach(println)
  }
}
