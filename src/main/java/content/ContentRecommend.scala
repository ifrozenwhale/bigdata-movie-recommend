package content

import constant.{Config, MongoConstant}
import model.{MongoConfig, Movie, MovieRecommendation, Recommendation}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix
object ContentRecommend {
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix):Double ={
    movie1.dot(movie2) / ( movie1.norm2() * movie2.norm2() )
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster(Config.SPARK_CORES).setAppName("AlsRecommender")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    implicit val mongoConfig = MongoConfig(Config.MONGO_URI, Config.MONGO_DB)
    val movieDF = sparkSession.read.option("uri", mongoConfig.uri).option("collection", MongoConstant.MOVIE_COL)
      .format("com.mongodb.spark.sql").load().as[Movie].limit(10000).rdd
      .map(x => (x.movieId, x.title, x.genres.map(e => if(e == '|') ' ' else e)))
      .toDF("movieId", "name", "genres").cache()
    val tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words")
    val words = tokenizer.transform(movieDF)
    // 引入HashingTF工具，可以把一个词语序列转化成对应的词频
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(50)
    val featurizedData = hashingTF.transform(words)

    // 引入IDF工具，可以得到idf模型
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // 训练idf模型，得到每个词的逆文档频率
    val idfModel = idf.fit(featurizedData)
    // 用模型对原数据进行处理，得到文档中每个词的tf-idf，作为新的特征向量
    val rescaledData = idfModel.transform(featurizedData)

    //    rescaledData.show(truncate = false)

    val movieFeatures = rescaledData.map(
      row => ( row.getAs[Int]("movieId"), row.getAs[SparseVector]("features").toArray )
    )
      .rdd
      .map(
        x => ( x._1, new DoubleMatrix(x._2) )
      )
    movieFeatures.collect().foreach(println)

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
      .filter(_._2._2 > 0.6)    // 过滤出相似度大于0.6的
      .groupByKey()
      .map{
        case (mid, items) => MovieRecommendation( mid, items.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)) )
      }
      .toDF()
    movieRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", MongoConstant.CONTENT_RECS_COL)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    sparkSession.stop()
  }
}
