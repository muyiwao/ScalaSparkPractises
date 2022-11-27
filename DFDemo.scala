import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.{SparkConf, SparkContext}

object DFDemo extends App {
  //def main(args: Array[String]): Unit = {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "DFDemo")
  sparkconf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkconf).getOrCreate()

  // way1 for explicit schema - DDL string
  //val ordersDDL = "orderid Int, orderdate String, custid Int, orderstatus String"

  //way2 - programmatic
  val ordersSchema = StructType (List(
    StructField ("orderid", IntegerType, true),
    StructField ("orderdate", TimestampType, true),
    StructField ("custid", IntegerType, true),
    StructField ("orderstatus", StringType, true)
  ))

  val orderDF = spark.read.option("header", true)
                     //.option("inferSchema", true) // not recommended in production
                     .schema(ordersSchema)
                     .csv(path="C:/Demos/input/orders.csv")
  /*
  print(orderDF.rdd.getNumPartitions)
  //repartition
  orderDF.repartition(2)
  //print(orderDF.)*/

  //orderDF.printSchema()
  //orderDF.show()

  //  Filter dataset to get dataframe
  // .select(col="orderid", cols = "orderstatus","custid")
  // .count()
  val processedDF = orderDF.where(conditionExpr="custid>10000")
                           .select(col="orderid", cols = "orderstatus","custid")
                           .groupBy("custid")
                           .count()


  //processedDF.show()
  //orderDF.show()

  //how to write output data in a file
  //orderDF.write.format("csv").mode(SaveMode.Overwrite).option("path", "C:/Demos/output23").save()

  //orderDF.write.format("csv").mode(SaveMode.Overwrite).saveAsTable("order")


  spark.stop()
  //scala.io.StdIn.readLine()
  //}

}