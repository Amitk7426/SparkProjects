package com.myCompany.scalaExample

import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

object SparkMongoConnect extends App {

  val conf = new SparkConf()
    .setAppName("SparkMongoConnect")
    .setMaster("local[1]")

  val sc = new SparkContext(conf)

  /* mongodb://127.0.0.1/testMongodb.movie
  lost name : 127.0.0.1/
  *  Database : IO
  *  Table/Collection : postcomments */
  val readConfig = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/testMongodb.movie")) // 1)
  val zipDf = sc.loadFromMongoDB(readConfig).toDF() // 2)

  zipDf.printSchema() // 3)
  zipDf.show()

  var name = zipDf
  
  // Writing data in MongoDB:
  // Version greater than or equal to MongoDB 3.2 will be only supported
  MongoSpark
    .write(name)
    .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/testMongodb" )
    .option("collection","movieTest")
    .mode("overwrite")
    .save()
}