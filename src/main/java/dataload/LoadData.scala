package dataload

import com.mongodb.casbah.Imports.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import model.{MongoConfig, Movie, Rating, Tag}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import constant.MongoConstant
import constant.Config
object LoadData{
  Logger.getLogger("org").setLevel(Level.WARN)

  val HDFS_PATH = "hdfs://localhost:9000/project/data/"
  val MOVIE_DATA_NAME = "movies.csv"
  val TAG_DATA_NAME = "tags.csv"
  val RATING_DATA_NAME = "ratings.csv"

  val MOVIE_DATA_PATH = HDFS_PATH + MOVIE_DATA_NAME
  val TAG_DATA_PATH = HDFS_PATH + TAG_DATA_NAME
  val RATING_DATA_PATH = HDFS_PATH + RATING_DATA_NAME

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster(Config.SPARK_CORES).setAppName("DataLoad")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


//    import sparkSession.implicits._
//    val movieRDD = sparkSession.sparkContext.textFile(MOVIE_DATA_PATH)

//    val movieDF = movieRDD.zipWithIndex().filter(_._2>0).keys.map(
//      elem => {
//        val attr = elem.split(",")
//        model.Movie(attr(0).toInt, attr(1).trim, attr(2))
//      }
//    ).toDF()
//    movieDF.limit(10).show()
//
//    val ratingRDD = sparkSession.sparkContext.textFile(RATING_DATA_PATH)
//    val ratingDF = ratingRDD.zipWithIndex().filter(_._2>0).keys.map(
//      elem => {
//        val attr = elem.split(",")
//        model.Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
//      }
//    ).toDF()

//    val tagRDD = sparkSession.sparkContext.textFile(TAG_DATA_PATH)


//    val tagDF = tagRDD.zipWithIndex().filter(_._2>0).keys.map(
//      elem => {
//        val attr = elem.split(",")
//        Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
//      }
//    ).toDF()

    val movieDF = sparkSession.read.format("csv")
      .option("header", "true")
      .schema(ScalaReflection.schemaFor[Movie].dataType.asInstanceOf[StructType])
      .load(MOVIE_DATA_PATH)

    movieDF.limit(15).show()

    val ratingDF = sparkSession.read.format("csv")
      .option("header", "true")
      .schema(ScalaReflection.schemaFor[Rating].dataType.asInstanceOf[StructType])
      .load(RATING_DATA_PATH)

    ratingDF.limit(5).show()
    val tagDF = sparkSession.read.format("csv")
      .option("header", "true")
      .schema(ScalaReflection.schemaFor[Tag].dataType.asInstanceOf[StructType])
      .load(TAG_DATA_PATH)

      tagDF.limit(5).show()


    implicit val mongoConfig = MongoConfig(Config.MONGO_URI, Config.MONGO_DB)
    writeToMongoDB(movieDF, ratingDF, tagDF)
  }

  def writeToMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit ={

    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    mongoClient(mongoConfig.db)(MongoConstant.MOVIE_COL).dropCollection()
    mongoClient(mongoConfig.db)(MongoConstant.RATING_COL).dropCollection()
    mongoClient(mongoConfig.db)(MongoConstant.TAG_COL).dropCollection()

    def writeDF(DF: DataFrame, COL: String): Unit ={
      DF.write.option("uri", mongoConfig.uri)
        .option("collection", COL)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()
    }
    writeDF(movieDF, MongoConstant.MOVIE_COL)
    writeDF(ratingDF, MongoConstant.RATING_COL)
    writeDF(tagDF, MongoConstant.TAG_COL)

    // index
    def createTableIndex(COL: String, key: String): Unit={
      mongoClient(mongoConfig.db)(COL).createIndex(MongoDBObject(key -> 1))
    }
    createTableIndex(MongoConstant.MOVIE_COL, "movieId")
    createTableIndex(MongoConstant.RATING_COL, "userId")
    createTableIndex(MongoConstant.RATING_COL, "movieId")
    createTableIndex(MongoConstant.TAG_COL, "movieId")
    createTableIndex(MongoConstant.TAG_COL, "userId")
    mongoClient.close()
  }
}