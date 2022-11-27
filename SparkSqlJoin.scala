import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSqlJoin extends App {
  Logger.getLogger("org").setLevel (Level.ERROR)

  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "DFDemo")
  sparkconf.set("spark.master", "local[*]")
  val spark = SparkSession.builder().config(sparkconf).getOrCreate()

  val orderDf = spark.read.option("header", true)
    .option("inferSchema", true)
    .csv("C:/Demos/input/order_data.csv")

  val CustomerDf = spark.read.option("header", true)
    .option("inferSchema", true)
    .csv("C:/Demos/input/customer_data.csv")


  orderDf.createOrReplaceTempView("Orders")
  CustomerDf.createOrReplaceTempView("Customers")

  // Join using Spark-Sql table
  val res = spark.sql("select Orders.OrderID, Customers.CustomerName,Orders.OrderDate " +
                              "from Orders " +
                              "inner join Customers " +
                              "on Orders.CustomerID = Customers.CustomerID")

  res.show()


}