package spark.transformation

import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object CommandFlatMap extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("...")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

//  create an RDD by loading the data from a Seq collection
  val data = Seq("Project Gutenberg’s",
    "Alice’s Adventures in Wonderland",
    "Project Gutenberg’s",
    "Adventures in Wonderland",
    "Project Gutenberg’s")
  val rdd = spark.sparkContext.parallelize(data)
  rdd.foreach(println)

//  flatmap
  val rdd1 = rdd.flatMap(f => f.split(" "))
  rdd1.foreach(println)

println("___________________________________________________________________")

//  flatmap on DataFrame
  val arrayStructureData = Seq(
    Row("James,,Smith", List("Java", "Scala", "C++"), "CA"),
    Row("Michael,Rose,", List("Spark", "Java", "C++"), "NJ"),
    Row("Robert,,Williams", List("CSharp", "VB", "R"), "NV")
  )

  val arrayStructureSchema = new StructType()
    .add("name", StringType)
    .add("languagesAtSchool", ArrayType(StringType))
    .add("currentState", StringType)

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructureData), arrayStructureSchema)

  import spark.implicits._

  //flatMap() Usage
  val df2 = df.flatMap(f => f.getSeq[String](1).map((f.getString(0), _, f.getString(2))))
    .toDF("Name", "language", "State")

  df2.show(false)
}
