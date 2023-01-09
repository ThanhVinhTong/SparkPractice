package spark

import org.apache.spark.sql.SparkSession

object AccumulatorsVariables extends App {
  val spark = SparkSession.builder()
    .master("local")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

//  Long accumulator
  val accum = sc.longAccumulator("SumAccumulator")
  sc.parallelize(Array(1,2,3)).foreach(x => accum.add(x))
  println(accum.value)

}
