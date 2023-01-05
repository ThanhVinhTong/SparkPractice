package spark.action

import org.apache.spark.sql.SparkSession

object SparkActionFirstTopMinMax extends App{
  val spark = SparkSession.builder()
    .master("local")
    .appName("...")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

//  Create Rdd
  val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))
  val inputRDD = spark.sparkContext.parallelize(List(("A", 1), ("B", 20), ("C", 30), ("D", 40), ("B", 50), ("B", 60)))

//  Action First
  println("first :  " + listRdd.first())
  //Output: first :  1
  println("first :  " + inputRDD.first())
  //Output: first :  (Z,1)

//  Action Top
  println("top : " + listRdd.top(2).mkString(","))
  //Output: top : 5,4
  println("top : " + inputRDD.top(2).mkString(","))
  //Output: top : (D,40), (C,30)

  //Note: Use this method only when the resulting array is small, as all the data is loaded into the driver’s memory

//  Action Min
  println("min :  " + listRdd.min())
  //Output: min :  1
  println("min :  " + inputRDD.min())
  //Output: min :  (A,1)

//  Action Max
  println("max :  " + listRdd.max())
  //Output: max :  5
  println("max :  " + inputRDD.max())
  //Output: max :  (D,40)
}
