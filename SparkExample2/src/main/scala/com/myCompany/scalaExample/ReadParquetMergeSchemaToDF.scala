package com.myCompany.scalaExample

import org.apache.spark.{SparkConf, SparkContext}

object ReadParquetMergeSchemaToDF {
  case class textCase(date: String, A: Float, B:Float, C:Float, D:Float, E:Long, F:Float)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ReadParquetMergeSchemaToDF").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    // Create a simple DataFrame, stored into a partition directory
    val df1 = sc.makeRDD(1 to 5).map(i => (i, i * 2)).toDF("single", "double")
    df1.write.parquet("output/parquet/data/test_table/key=1")

    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column
    val df2 = sc.makeRDD(6 to 10).map(i => (i, i * 3)).toDF("single", "triple")
    df2.write.parquet("output/parquet/data/test_table/key=2")

    // Read the partitioned table
    val df3 = sqlContext.read.option("mergeSchema", "true").parquet("output/parquet/data/test_table")
    df3.printSchema()
    df3.show()

    sc.stop
  }
}
