import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSqlDemo extends App {
  Logger.getLogger("org").setLevel (Level.ERROR)

  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "DFDemo")
  sparkconf.set("spark.master", "local[*]")
  val spark = SparkSession.builder().config(sparkconf).getOrCreate()

  val orderDF = spark.read.option ("header", true)
    .option("inferSchema", true) // not recommended in production
    .csv(path="C:/Demos/input/orders.csv")

  //this table is just for quering , stored temporary
  orderDF.createOrReplaceTempView("tablenameOrder")
  //val res = spark.sql("any sql query)
  val res = spark.sql("select order_status, count(*) as totOrders from tablenameOrder group by order_status")

  res.show()



}
