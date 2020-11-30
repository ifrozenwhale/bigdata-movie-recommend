package constant

object MongoConstant {
  val MOVIE_COL = "Movie"
  val RATING_COL = "Rating"
  val TAG_COL = "Tag"
  val HOT_MOVIES_COL = "HotMovie" // 热门电影 评分数top
  val HOT_MOVIES_MONTH_COL = "HotMovieMonth" // 分月份的热门电影 评分数top
  val TOP_MOVIES_COL = "TopMovie" // 高评分电影
  val TOP_MOVIES_GENRES_COL = "TopMovieGenres" // 分类别评分电影

  val TOP_HOT_MOVIES_COL = "TopHotMovie" // 高分电影 评论树大于100
  val USER_RECS_COL = "UserRecs"
  val MOVIE_RECS_COL = "MovieRecs"
  val USER_RECOMMENDER_NUMBER = 20
  val CONTENT_RECS_COL = "MovieContentRecs"
}
