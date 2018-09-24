package com.myCompany.scalaExample

import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsOperation {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MapPartitionsOperation").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val testRDD = sc.parallelize(List(
      "yellow", "red",
      "blue", "cyan",
      "black"
    ),
      3)
    //testRDD.map(p=>(p,1)).foreach(println)


    val mapped = testRDD.mapPartitionsWithIndex {
      // 'index' represents the Partition No
      // 'iterator' to iterate through all elements
      //                         in the partition
      (index, iterator) => {
        println("Called in Partition -> " + index)
        println("Called in Partition iterator-> " + iterator.toList)
        val myList = iterator.toList
        // In a normal user case, we will do the
        // the initialization(ex : initializing database)
        // before iterating through each element
        myList.map(x => x + " -> " + index).iterator
      }
    }
    mapped.collect()

    val mapped2 = testRDD.mapPartitions {
      // 'index' represents the Partition No
      // 'iterator' to iterate through all elements
      //                         in the partition
      (iterator) => {

        println("Called in Partition iterator2-> " + iterator.toList)
        val myList = iterator.toList
        // In a normal user case, we will do the
        // the initialization(ex : initializing database)
        // before iterating through each element
        myList.iterator
      }
    }
    mapped2.collect()
  }
}

