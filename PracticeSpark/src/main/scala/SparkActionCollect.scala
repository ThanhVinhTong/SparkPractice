import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

//avoid when  large dataset because it returns entire dataset

object SparkActionCollect extends App {
  val spark = SparkSession.builder()
    .appName("...")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

//  Create Rdd
  val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))

//  Collect from Rdd
  val data:Array[Int] = listRdd.collect()
  data.foreach(println)
  listRdd.foreach(x => println("key=" + x))

// Create DataFrame
  val rawData = Seq(Row(Row("James ", "Hehe", "Smith"), "36636", "M", 3000),
    Row(Row("Michael ", "Rose", "Ehe"), "40288", "M", 4000),
    Row(Row("Robert ", "HE", "Williams"), "42114", "M", 4000),
    Row(Row("Maria ", "Anne", "Jones"), "39192", "F", 4000),
    Row(Row("Jen", "Mary", "Brown"), "", "F", -1)
  )

  //  Create Schema
  val schema = new StructType()
    .add("name", new StructType()
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType))
    .add("id", StringType)
    .add("gender", StringType)
    .add("salary", IntegerType)

  val df = spark.createDataFrame(spark.sparkContext.parallelize(rawData), schema)
  df.printSchema()
  df.show(false)

//  Collect from data frame
  val collectedData = df.collect()
  collectedData.foreach(row =>{
    val salary = row.getInt(3)
    println(salary)
  })

//  Retrieving data from struct column
  collectedData.foreach(row =>{
    val salary = row.getInt(3)
    val fullName:Row = row.getStruct(0)
    val firstName = fullName.getString(0)
    val middleName = fullName.get(1).toString
    val lastName = fullName.getAs[String]("lastname")
    println(firstName + " " + middleName + " " + lastName + " " + salary)
  })

}
