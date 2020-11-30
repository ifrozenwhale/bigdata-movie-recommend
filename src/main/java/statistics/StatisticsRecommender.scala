package statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import constant.{Config, MongoConstant}
import model.{GenresRecommendation, MongoConfig, Movie, Rating, Recommendation}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.Properties
object StatisticsRecommender {
  Logger.getLogger("org").setLevel(Level.WARN)
  val sparkConf = new SparkConf().setMaster(Config.SPARK_CORES).setAppName("StatisticRecommender")
  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  import sparkSession.implicits._
  implicit val mongoConfig = MongoConfig(Config.MONGO_URI, Config.MONGO_DB)
  val ratingDF = sparkSession.read
    .option("uri", mongoConfig.uri)
    .option("collection", MongoConstant.RATING_COL)
    .format("com.mongodb.spark.sql").load().as[Rating].toDF()
  val movieDF = sparkSession.read
    .option("uri", mongoConfig.uri)
    .option("collection", MongoConstant.MOVIE_COL)
    .format("com.mongodb.spark.sql").load().as[Movie].toDF()

  def main(args: Array[String]): Unit = {
    val startTime: Long = System.currentTimeMillis
    Properties.setProp("scala.time","true")
    // finished
    // tmp ratings use memory

    // 1. hot
//    hotRecommend()
    // 2. hot by month
//    hotMonthRecommend()
    // 3. top
//    topRecommend()
    // 4, top by genres
//    topGenresRecommend()
    // 5. top and hot Recommend

    topHotRecommend()

    sparkSession.stop()
    val endTime: Long = System.currentTimeMillis
    System.out.println("total time： " + (endTime - startTime) / 1000.0 + "s")
  }

  def hotRecommend(): Unit={
    ratingDF.createOrReplaceTempView("RatingsTmp")
    val hotMovieDF = sparkSession.sql("select movieId, count(movieId) as count from RatingsTmp group by movieId order by count desc")
    hotMovieDF.limit(10).show()
    writeToMongoDB(hotMovieDF, MongoConstant.HOT_MOVIES_COL)
  }

  def hotMonthRecommend(): Unit={
    // change date format to yyyyMM
    val sdf = new SimpleDateFormat("yyyyMM")
    sparkSession.udf.register("changeDate", (x: Int)=>sdf.format(new Date(x * 1000L)).toInt)
    // tmp ratings of months use memory
    ratingDF.createOrReplaceTempView("RatingsTmp")
    val ratingMonthDF = sparkSession.sql("select movieId, rating, changeDate(timestamp) as yearmonth from RatingsTmp")
    ratingMonthDF.createOrReplaceTempView("RatingsMonthTmp")
    val hotMovieMonthDF = sparkSession.sql("select movieId, count(movieId) as count, yearmonth from RatingsMonthTmp group by yearmonth, movieId order by yearmonth desc, count desc")
    hotMovieMonthDF.limit(10).show()
    writeToMongoDB(hotMovieMonthDF, MongoConstant.HOT_MOVIES_MONTH_COL)
  }

  def topRecommend(): Unit={
    ratingDF.createOrReplaceTempView("RatingsTmp")
    val movieAvgRatingDF = sparkSession.sql("select movieId, avg(rating) as avg from RatingsTmp group by movieId")
    movieAvgRatingDF.limit(10).show()
    writeToMongoDB(movieAvgRatingDF, MongoConstant.TOP_MOVIES_COL)

  }

  def topGenresRecommend(): Unit={
    // add avg ratings to movie collection
    ratingDF.createOrReplaceTempView("RatingsTmp")
    val movieAvgRatingDF = sparkSession.sql("select movieId, avg(rating) as avg from RatingsTmp group by movieId")
    val movieWithRating = movieDF.join(movieAvgRatingDF, "movieId")
    // mid genres rating
    // 1 a|b|c 9
    // 2 a|c 8
    // 3 b 10
    // detail see in the .md
    // first change genres to rdd
    // genres in readme.txt
    val genres = List("Action", "Adventure", "Animation", "Children's", "Comedy", "Crime", "Documentary", "Drama", "Fantasy",
      "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western")
    val genresRDD = sparkSession.sparkContext.makeRDD(genres)
    val genresTopMoviesDF = genresRDD.cartesian(movieWithRating.rdd)
      .filter{case(genre, movie) => movie.getAs[String]("genres").toLowerCase.contains(genre.toLowerCase()) }
      .map{case (genre, movie) => (genre, (movie.getAs[Int]("movieId"), movie.getAs[Double]("avg")))}
      .groupByKey() // (genre, (mid, avg)) => (genre, [(mid1, avg1), (mid2, avg2), ..., (midn, avgn)]
      .map{case(genre, items) => GenresRecommendation(genre, items.toList.sortWith(_._2>_._2).take(50).map(item => Recommendation(item._1, item._2)))
      }.toDF()
    genresTopMoviesDF.show()
    writeToMongoDB(genresTopMoviesDF, MongoConstant.TOP_MOVIES_GENRES_COL)
  }
  def topHotRecommend(): Unit={
    ratingDF.createOrReplaceTempView("RatingsTmp")
    movieDF.createOrReplaceTempView("MoviesTmp")
    val topMoviesDF = sparkSession.sql("select movieId, count(movieId) as count from RatingsTmp group by movieId")
    val hotMovieDF = sparkSession.sql("select movieId, avg(rating) as avg from RatingsTmp group by movieId")
    val topHotMovieDF = topMoviesDF.join(hotMovieDF, "movieId").join(sparkSession.sql("select movieId, title from MoviesTmp"), "movieId")
    topHotMovieDF.limit(10).show()
    writeToMongoDB(topHotMovieDF, MongoConstant.TOP_HOT_MOVIES_COL)
  }
  def writeToMongoDB(df: DataFrame, collectionName: String)(implicit mongoConfig: MongoConfig):Unit={
    df.write.option("uri", mongoConfig.uri).option("collection", collectionName).mode("overwrite")
      .format("com.mongodb.spark.sql").save()
  }
}
