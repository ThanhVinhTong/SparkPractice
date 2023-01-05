import org.apache.spark.sql.SparkSession

object SparkActionAggregate extends App {
//  create an RDD
  val spark = SparkSession.builder()
    .appName("SparkActions")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  val inputRDD = spark.sparkContext.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))
  val listRDD = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))

//  aggregate action
  def param0 = (accu: Int, v: Int) => accu + v
  def param1 = (accu1: Int, accu2: Int) => accu1 + accu2
  println("aggregate : " + listRDD.aggregate(0)(param0, param1))
  //Output: aggregate : 20

  def param3 = (accu: Int, v: (String, Int)) => accu + v._2
  println(param3)
  def param4 = (accu1: Int, accu2: Int) => accu1 + accu2
  println("aggregate : " + inputRDD.aggregate(0)(param3, param4))
  //Output: aggregate : 181
}
