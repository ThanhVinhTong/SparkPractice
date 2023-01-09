package spark.sql

import org.apache.spark.sql.SparkSession

object SparkSQLConnectToMySQL extends App {
  val spark = SparkSession.builder()
    .appName("Create DataFrame From Xml File")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val mysql_db_driver_class = "com.mysql.cj.jdbc.Driver"
  val table_name = "countries"
  val host_name = "localhost"
  val port_no = "3306"
  val user_name = "root"
  val password = "12345678"
  val database_name = "hr"

  val mysql_select_query = "(select * from %s) as country".format(table_name)
  val mysql_jdbc_url = "jdbc:mysql://%s:%s/%s".format(host_name, port_no, database_name)

  val user_data_df = spark.read.format("jdbc")
    .option("url", mysql_jdbc_url)
    .option("driver", mysql_db_driver_class)
    .option("dbtable", mysql_select_query)
    .option("user", user_name)
    .option("password", password)
    .load()
  user_data_df.show(10, false)

  spark.stop()
  println("completed")
}

/*
jdbc:mysql://localhost:3306/hr
+----------+------------+---------+
|COUNTRY_ID|COUNTRY_NAME|REGION_ID|
+----------+------------+---------+
|AR        |Argentina   |2        |
|AU        |Australia   |3        |
|BE        |Belgium     |1        |
|BR        |Brazil      |2        |
|CA        |Canada      |2        |
|CH        |Switzerland |1        |
|CN        |China       |3        |
|DE        |Germany     |1        |
|DK        |Denmark     |1        |
|EG        |Egypt       |4        |
+----------+------------+---------+
 */