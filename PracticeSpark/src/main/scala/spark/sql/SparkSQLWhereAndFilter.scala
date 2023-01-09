package spark.sql

import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SparkSQLWhereAndFilter extends App {
  val spark = SparkSession.builder()
    .master("local[5]")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

//  Create dataframe
  val arrayStructureData = Seq(
    Row(Row("James", "", "Smith"), List("Java", "Scala", "C++"), "OH", "M"),
    Row(Row("Anna", "Rose", ""), List("Spark", "Java", "C++"), "NY", "F"),
    Row(Row("Julia", "", "Williams"), List("CSharp", "VB"), "OH", "F"),
    Row(Row("Maria", "Anne", "Jones"), List("CSharp", "VB"), "NY", "M"),
    Row(Row("Jen", "Mary", "Brown"), List("CSharp", "VB"), "NY", "M"),
    Row(Row("Mike", "Mary", "Williams"), List("Python", "VB"), "OH", "M")
  )

  val arrayStructureSchema = new StructType()
    .add("name", new StructType()
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType))
    .add("languages", ArrayType(StringType))
    .add("state", StringType)
    .add("gender", StringType)

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructureData), arrayStructureSchema)
  df.printSchema()
  df.show()

// where with condition
  df.where(df("state") === "OH")
    .show(false)
  df.where("gender == 'M'")
    .show(false)

// Filtering with multiple conditions
  df.where("state == 'OH' AND gender == 'M'")
    .show(false)

  df.where("ARRAY_CONTAINS(languages, 'Java')")
    .show(false)

  df.where("name.firstname LIKE 'J%'")
    .show(false)
}
