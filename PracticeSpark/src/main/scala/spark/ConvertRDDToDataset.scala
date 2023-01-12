package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ConvertRDDToDataset extends App {
  val spark = SparkSession.builder()
    .master("local[3]")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._

//  Create RDD:
  val columns = Seq("language", "users_count")
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
  val rdd = sc.parallelize(data)

  println(columns(0), columns(1))
//  convert RDD to DataFrame
  val dfFromRDD = spark.createDataFrame(rdd).toDF(columns:_*)
  dfFromRDD.show()

//  Convert RDD to Dataset
  val ds = spark.createDataset(rdd).toDF(columns:_*)
  ds.show()

}
