package spark.action

import org.apache.spark.sql.SparkSession

object SparkActionTake extends App{
  val spark = SparkSession.builder()
    .master("local")
    .appName("...")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

//  Create Rdd
  val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))

  //take, takeOrdered, takeSample
  println("take : " + listRdd.take(2).mkString(","))
  println("takeOrdered : " + listRdd.takeOrdered(2).mkString(","))
}
