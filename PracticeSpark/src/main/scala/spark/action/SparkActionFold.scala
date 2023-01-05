package spark.action

import org.apache.spark.sql.SparkSession

object SparkActionFold extends App{
  val spark = SparkSession.builder()
    .appName("SparkPractice")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

//  Create RDD
  val listRDD = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5, 3, 2))
  val inputRDD = spark.sparkContext.parallelize(List(("A", 1), ("B", 20), ("C", 30), ("B", 40), ("B", 50)))

//  fold
  println("fold : " + listRDD.fold(0){ (acc, v) =>
    val sum = acc+v
    sum
  })

  println("fold : " + inputRDD.fold(("Total", 0)){(acc:(String,Int), v:(String,Int))=>
    val sum = acc._2 + v._2
    ("Total", sum)
  })
}
