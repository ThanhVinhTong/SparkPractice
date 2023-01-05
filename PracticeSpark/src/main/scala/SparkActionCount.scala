import org.apache.spark.sql.SparkSession

object SparkActionCount extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("...")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
//  create Rdd
  val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))
  val inputRDD = spark.sparkContext.parallelize(List(("A", 1), ("B", 20), ("C", 30), ("D", 40), ("B", 50), ("B", 60)))

  //count, countApprox, countApproxDistinct
  println("Count : " + listRdd.count)
  //Output: Count : 7
  println("countApprox : " + listRdd.countApprox(1200))
  //Output: countApprox : (final: [7.000, 7.000])
  println("countApproxDistinct : " + listRdd.countApproxDistinct())
  //Output: countApproxDistinct : 5
  println("countApproxDistinct : " + inputRDD.countApproxDistinct())
  //Output: countApproxDistinct : 6

  //countByValue, countByValueApprox
  println("countByValue :  " + listRdd.countByValue())
  //Output: countByValue :  Map(5 -> 1, 1 -> 1, 2 -> 2, 3 -> 2, 4 -> 1)
  //println(listRdd.countByValueApprox()
}
