import org.apache.spark.sql.SparkSession

object WriteReadHDFS extends App {
  val spark = SparkSession.builder()
    .appName("...")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val df = spark.read.csv("./data/text01.csv")
  df.write.option("header","true").csv("hdfs://localhost:9000/testhdfs.csv")

//  val df2 = spark.read.csv("hdfs://localhost:9000/text02.csv")
//  df2.write.option("header", "true").csv("./data/textDownloaded.csv")
}
