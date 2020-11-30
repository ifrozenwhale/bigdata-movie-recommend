package als

import constant.{Config, MongoConstant}
import model.{MongoConfig, MovieRecommendation, Recommendation, UserRecommendation}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

object alsRecommender {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster(Config.SPARK_CORES).setAppName("AlsRecommender")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    implicit val mongoConfig = MongoConfig(Config.MONGO_URI, Config.MONGO_DB)
    val ratingRDD = sparkSession.read.option("uri", mongoConfig.uri).option("collection", MongoConstant.RATING_COL)
      .format("com.mongodb.spark.sql").load().as[model.Rating].limit(500000).rdd
      .map(rating => (rating.userId, rating.userId, rating.rating))
      .cache()

    // 从rating数据中提取所有的uid和mid，并去重
    val userRDD = ratingRDD.map(_._1).distinct()
    val movieRDD = ratingRDD.map(_._2).distinct()

    // 训练隐语义模型
    val trainData = ratingRDD.map( x => Rating(x._1, x._2, x._3) )

    val (rank, iterations, lambda) = (200, 5, 0.1)
    val alsModel = ALS.train(trainData, rank, iterations, lambda)

    // 基于用户和电影的隐特征，计算预测评分，得到用户的推荐列表
    // 计算user和movie的笛卡尔积，得到一个空评分矩阵
    val userMovies = userRDD.cartesian(movieRDD)

    // 调用model的predict方法预测评分
    val preRatings = alsModel.predict(userMovies)

//    val userRecs = preRatings
//      .filter(_.rating > 0)    // 过滤出评分大于0的项
//      .map(rating => ( rating.user, (rating.product, rating.rating) ) )
//      .groupByKey()
//      .map{
//        case (uid, recs) => UserRecommendation( uid, recs.toList.sortWith(_._2>_._2).take(MongoConstant.USER_RECOMMENDER_NUMBER).map(x=>Recommendation(x._1, x._2)) )
//      }
//      .toDF()
//
//    userRecs.write
//      .option("uri", mongoConfig.uri)
//      .option("collection", MongoConstant.USER_RECS_COL)
//      .mode("overwrite")
//      .format("com.mongodb.spark.sql")
//      .save()

    // 基于电影隐特征，计算相似度矩阵，得到电影的相似度列表
    val movieFeatures = alsModel.productFeatures.map{
      case (movieId, features) => (movieId, new DoubleMatrix(features))
    }

//    movieFeatures.saveAsTextFile("/home/frozenwhale/Desktop/bigdata/project/result/data/movieFeatures.txt")

    // 对所有电影两两计算它们的相似度，先做笛卡尔积
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter{
        // 把自己跟自己的配对过滤掉
        case (a, b) => a._1 != b._1
      }
      .map{
        case (a, b) => {
          val simScore = this.consinSim(a._2, b._2)
          ( a._1, ( b._1, simScore ) )
        }
      }
      .filter(_._2._2 > 0.2)    // 过滤出相似度大于0.6的
      .groupByKey()
      .map{
        case (mid, items) => MovieRecommendation( mid, items.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)) )
      }
      .toDF()


    movieRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", MongoConstant.MOVIE_RECS_COL)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    sparkSession.stop()

  }
  // 求向量余弦相似度
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix):Double ={
    movie1.dot(movie2) / ( movie1.norm2() * movie2.norm2() )
  }

}
