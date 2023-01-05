package spark.transformation

import org.apache.spark.sql.SparkSession

object CommandReduceByKey extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("...")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

//  Create RDD from a list
  val data = Seq(("Project", 1),
    ("Gutenberg’s", 1),
    ("Alice’s", 1),
    ("Adventures", 1),
    ("in", 1),
    ("Wonderland", 1),
    ("Project", 1),
    ("Gutenberg’s", 1),
    ("Adventures", 1),
    ("in", 1),
    ("Wonderland", 1),
    ("Project", 1),
    ("Gutenberg’s", 1))

  val rdd = spark.sparkContext.parallelize(data)

//  ReduceByKey
  val rdd2 = rdd.reduceByKey(_ + _)
  rdd2.foreach(println)
}
