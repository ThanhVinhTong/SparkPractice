package spark.sql

import org.apache.spark.sql.SparkSession

object SparkSQLSample extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("...")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /*
  DataFrame Sample()
   */
  val df = spark.range(100)
  println(df.sample(true,0.3,123).collect().mkString(",")) //with Duplicates
  // 0,5,9,11,14,14,16,17,21,29,33,41,42,52,52,54,58,65,65,71,76,79,85,96
  println(df.sample( false, 0.3,123).collect().mkString(",")) // No duplicates
  // 0,4,17,19,24,25,26,36,37,41,43,44,53,56,66,68,69,70,71,75,76,78,83,84,88,94,96,97,98

  /*
  RDD sample()
   */
  val rdd = spark.sparkContext.range(0,100)
  println(rdd.sample(false, 0.1, 0).collect().mkString(","))
  //Output: 1,20,29,42,53,62,63,70,82,87
  println(rdd.sample(true, 0.3, 123).collect().mkString(","))
  //Output: 1,4,21,30,32,32,32,33,42,45,46,52,53,54,55,58,58,66,66,68,70,70,74,86,92,96,98,99
}
