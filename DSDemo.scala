//import org.apache.arrow.flatbuf.Timestamp
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
case class ordersData (order_id:String, order_date:String, order_customer_id:Int, order_status:String)
object DSDemo extends App {
  //def main(args: Array[String]): Unit = {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "DFDemo")
  sparkconf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkconf).getOrCreate()

  //val ordersDDL = "orderid Int, order_date String, custid Int, orderstatus String"

  val orderDF = spark.read.option("header", true)
                     .option("inferSchema", true) // not recommended in production
                  // .schema(ordersSchema)
                     .csv(path="C:/Demos/input/orders.csv")

  import spark.implicits._
  val ordersDS = orderDF.as[ordersData]
  //ordersDS.filter(x => x.order_customer_id>10000).show(5)
  ordersDS.filter(x => x.order_status == "CLOSED").show(5)

  //ordersDS.show()

  spark.stop()


  //scala.io.StdIn.readLine()
  //}
}
