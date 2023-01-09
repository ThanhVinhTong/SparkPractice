package spark.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object CreateDataFrame extends App {
  val spark = SparkSession.builder()
    .master("local[3]")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._
//  val columns = Seq("language", "users_count")
//  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

//  toDF()
//  val rdd = sc.parallelize(data)
//  val dfFromRDD1 = rdd.toDF("language", "users_count")
//  dfFromRDD1.printSchema()

//  createDataFrame() with Row type
//  val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns:_*)
//  val schema = StructType( Array(
//                  StructField("language", StringType, true),
//                  StructField("users", StringType, true)
//                ))
//  val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2))
//  val dfFromRDD3 = spark.createDataFrame(rowRDD, schema)

//  from csv type
//  val df2 = spark.read.csv("data/text01.csv")

// from Mysql table
val mysql_select_query = "(select * from countries as countries )"
  val df_mysql = spark.read.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/hr")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("dbtable", mysql_select_query)
    .option("user", "root")
    .option("password", "12345678")
    .load()
  df_mysql.show()

  spark.stop()
}
