import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

object SparkMongoMain extends App {

  val conf = new SparkConf()
    .setAppName("SparkMongoMain")
    .setMaster("local[1]")

  val sc = new SparkContext(conf)

  /* mongodb://127.0.0.1/IO.postcomments
  lost name : 127.0.0.1/
  *  Database : IO
  *  Table/Collection : postcomments */
  val readConfig = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/IO.postcomments")) // 1)
  val zipDf = sc.loadFromMongoDB(readConfig).toDF() // 2)

  zipDf.printSchema() // 3)
  zipDf.show()

  var minMaxCities = zipDf
  
  // Writing data in MongoDB:
  MongoSpark
    .write(minMaxCities)
    .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/IO" )
    .option("collection","newpostcomments")
    .mode("overwrite")
    .save()
}