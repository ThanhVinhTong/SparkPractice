package spark.transformation

import org.apache.spark.sql.{SaveMode, SparkSession}

object CommandRepartition extends App {
  val spark = SparkSession.builder()
    .master("local[3]")
    .getOrCreate()
  val sc = spark.sparkContext

  sc.setLogLevel("ERROR")

//  RDD
  val rddOriginal = sc.parallelize(Range(0,20), 6)
  println("Original RDD size:" + rddOriginal.partitions.length)
  rddOriginal.saveAsTextFile("data/testRepartition/OriginalRDD")

  val rddRepartition = rddOriginal.repartition(10)
  println("Repartition RDD:" + rddRepartition.partitions.length)
  rddRepartition.saveAsTextFile("data/testRepartition/RepartitionRDD")

//  DataFrame
  val df = spark.range(0, 20)
  println(df.rdd.partitions.length)

  df.write.mode(SaveMode.Overwrite) csv ("data/testRepartition/OriginalDataFrame.csv")

  val df2 = df.repartition(6)
  println(df2.rdd.partitions.length)

  df.write.mode(SaveMode.Overwrite) csv ("data/testRepartition/RepartitionDataFrame.csv")
}
