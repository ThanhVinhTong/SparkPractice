package spark.sql

import org.apache.spark.sql.SparkSession

object SparkSQLJoin extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("...")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

//  create an emp and dept DataFrame’s
  val emp = Seq((1, "Smith", -1, "2018", "10", "M", 3000),
    (2, "Rose", 1, "2010", "20", "M", 4000),
    (3, "Williams", 1, "2010", "10", "M", 1000),
    (4, "Jones", 2, "2005", "10", "F", 2000),
    (5, "Brown", 2, "2010", "40", "", -1),
    (6, "Brown", 2, "2010", "50", "", -1)
  )
  val empColumns = Seq("emp_id", "name", "superior_emp_id", "year_joined",
    "emp_dept_id", "gender", "salary")

  import spark.sqlContext.implicits._

  val empDF = emp.toDF(empColumns: _*)
  empDF.show(false)

  val dept = Seq(("Finance", 10),
    ("Marketing", 20),
    ("Sales", 30),
    ("IT", 40)
  )

  val deptColumns = Seq("dept_name", "dept_id")
  val deptDF = dept.toDF(deptColumns: _*)
  deptDF.show(false)

//  inner join
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "inner")
    .show(false)

//  full outer join
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "outer")
    .show(false)
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "full")
    .show(false)
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "fullouter")
    .show(false)

//  left outer join
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "left")
    .show(false)
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftouter")
    .show(false)

//  right outer join
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "right")
    .show(false)
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "rightouter")
    .show(false)

//  SQL expression
  empDF.createOrReplaceTempView("EMP")
  deptDF.createOrReplaceTempView("DEPT")
  //SQL JOIN
  val joinDF = spark.sql("select * from EMP e join DEPT d on e.emp_dept_id == d.dept_id")
  joinDF.show(false)

  val joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id")
  joinDF2.show(false)
}
