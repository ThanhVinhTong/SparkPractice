package spark.action

import org.apache.spark.sql.SparkSession

object SparkActionReduce extends App {
  val spark = SparkSession.builder()
    .appName("SparkPractice")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

//  Create RDD
  val listRDD = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))
  val inputRDD = spark.sparkContext.parallelize(List(("A", 1), ("B", 30), ("C", 40), ("D", 50), ("B", 60), ("B", 20)))

  //reduce
  println("output min using binary : " + listRDD.reduce(_ min _))
  println("output max using binary : " + listRDD.reduce(_ max _))
  println("output sum using binary : " + listRDD.reduce(_ + _))
  //Output: reduce : 20
  println("reduce alternate : " + listRDD.reduce((x, y) => x + y))
  //Output: reduce alternate : 20
  println("reduce : " + inputRDD.reduce((x, y) => ("Total", x._2 + y._2)))
  //Output: reduce : (Total,181)

  //treeReduce. This is similar to reduce
  println("treeReduce : " + listRDD.treeReduce(_ + _))
  //Output: treeReduce : 20
}
