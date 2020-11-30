package als

import constant.{Config, MongoConstant}
import model.MongoConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

case class alsModel(rank: Int, iter: Int, lambda: Double, rmse: Double)
object AlsTrainModel {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster(Config.SPARK_CORES).setAppName("AlsRecommender")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    implicit val mongoConfig = MongoConfig(Config.MONGO_URI, Config.MONGO_DB)
    val ratingRDD = sparkSession.read.option("uri", mongoConfig.uri).option("collection", MongoConstant.RATING_COL)
      .format("com.mongodb.spark.sql").load().as[model.Rating].rdd
      .map(rating => Rating(rating.userId, rating.userId, rating.rating))
      .cache()

//    print(ratingRDD.saveAsTextFile("/home/frozenwhale/Desktop/bigdata/project/result/data/ratingRdd.txt"))
    val parts = ratingRDD.randomSplit(Array(0.7, 0.3))
    val trainingRDD = parts(0)
    val testRDD = parts(1)
    print("start to train")
    val bestAls = train(trainingRDD, testRDD)
    print(bestAls)
  }
  def train(traingRDD: RDD[Rating], testRDD: RDD[Rating]): alsModel= {
    //     三个参数 rank iter lambda
    val res = for (rank <- Array(100, 200); iter <- Array(5); lambda <- Array(0.01, 0.1)) yield{
      val model = ALS.train(traingRDD, rank, iter)
      val rmse = evaluate(model, testRDD)
      print(rmse)
      alsModel(rank, iter, lambda, rmse)
    }
    res.minBy(_.rmse)
  }
  def evaluate(model: MatrixFactorizationModel, data: RDD[Rating]): Double={
    val predictRatingRDD = model.predict(data.map(item => (item.user, item.product)))
    val predictRDD = predictRatingRDD.map(item => ((item.user, item.product), item.rating))
    val actualRDD = data.map(item => ((item.user, item.product), item.rating))
    val predictActualRDD = predictRDD.join(actualRDD)
    import scala.math._

    val rme = predictActualRDD.map(item => pow(item._2._2 - item._2._1, 2)).mean()
    sqrt(rme)
  }
}
