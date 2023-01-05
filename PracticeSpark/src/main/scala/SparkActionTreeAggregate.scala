import org.apache.spark.sql.SparkSession

object SparkActionTreeAggregate extends App {
//  Create an RDD
  val spark = SparkSession.builder()
    .appName("SparkPractice")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val listRDD = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))

  def param1 = (accu: Int,  v:Int) => accu + v
  def param2 = (accu1: Int, accu2: Int) => accu1 + accu2
  println("treeAggregate : "+listRDD.treeAggregate(0)(param1, param2))
}
