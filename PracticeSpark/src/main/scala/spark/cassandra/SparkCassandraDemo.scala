package spark.cassandra

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

object SparkCassandraDemo extends App {
  private val conf = new SparkConf().setAppName("MySparkCassandraDemo")
                            .setMaster("local")
                            .set("spark.cassandra.connection.host", "localhost")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

//  define keyspace and table to use
  private val keyspace = "test"
  private val table = "emp"

//  Get data to RDD from Cassandra
  private val my_rdd = sc.cassandraTable(keyspace, table)
  println(my_rdd)
  my_rdd.foreach(println)

//  Update data from RDD to Cassandra
  private val new_rdd = sc.parallelize(Seq((4, "Ehe", "He He", 1234567890, 100000)))
  new_rdd.foreach(println)
  new_rdd.saveToCassandra(keyspace, table, SomeColumns("emp_id", "emp_city", "emp_name", "emp_phone", "emp_sal"))

  sc.stop()
}
