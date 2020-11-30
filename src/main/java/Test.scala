object Test {

  print("hello world")
  def main(args: Array[String]): Unit = {
    val startTime: Long = System.currentTimeMillis
    for (i <- 1 to 10; j <- 1 to 10; k <- 1 to 10){
      print(i, j, k)

    }
    val endTime: Long = System.currentTimeMillis
    System.out.println("total time： " + (endTime - startTime) / 1000.0 + "s")
  }
}
