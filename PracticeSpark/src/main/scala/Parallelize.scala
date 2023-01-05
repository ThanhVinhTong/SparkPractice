import org.apache.spark.sql.SparkSession

object Parallelize {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]")
                                      .appName("SparkByExamples.com")
                                      .getOrCreate()
    val rdd = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5))
    val rddCollect: Array[Int] = rdd.collect()

//    spark.sparkContext.setLogLevel("ERROR")

    println("Number of Partitions: " + rdd.getNumPartitions)
    println("Action: First element: " + rdd.first())
    println("Action: RDD converted to Array[Int] : ")
    rddCollect.foreach(println)
  }
}
