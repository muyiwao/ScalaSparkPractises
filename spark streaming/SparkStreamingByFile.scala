import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object SparkStreamingByFile extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .master(master = "local[2]")
    .appName(name = "My Streaming Application")
    .config("spark.sql.shuffle.partitions", 3)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  //1.Read from file source
  val ordersDf = spark.readStream
    .format(source = "json")
    .option("path", "input")
    .load()

  // 2. process
  ordersDf.createOrReplaceTempView(viewName = "orders")
  val completedOrders = spark.sql(sqlText = "select * from orders where order_status = 'COMPLETE'")

  //3.Write to the sink
  val ordersQuery = completedOrders.writeStream
      .format(source = "json")
      .outputMode(outputMode = "append")
      .option("path", "output")
      .option("checkpointLocation", "checkpoint-location5")
      .trigger(Trigger.ProcessingTime ("30 seconds"))
      .start()
  ordersQuery.awaitTermination()
}
