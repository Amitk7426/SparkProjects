package com.myCompany.scalaExample

import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object HashPartitioner {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("HashPartitioner").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(for {
      x <- 1 to 3
      y <- 1 to 2
    } yield (x, None), 8)

    rdd.foreach(println)
    println(rdd.partitioner)

    countByPartition(rdd).foreach(println)

    println(rdd.partitions.length)

    import org.apache.spark.HashPartitioner
    val rddOneP = rdd.partitionBy(new HashPartitioner(8))

    println(rddOneP.partitions.length)

    println(rddOneP.partitioner)

    countByPartition(rddOneP).foreach(println)

    sc.stop
  }

  import org.apache.spark.rdd.RDD

  def countByPartition(rdd: RDD[(Int, None.type)]) = {
    rdd.mapPartitions(iter => Iterator(iter.length))
  }
}