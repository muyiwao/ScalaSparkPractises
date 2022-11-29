import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


object StructuredStreamingWC extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .master(master = "local[2]")
    .appName(name = "My Streaming Application") //.config("spark.sql.shuffle.partitions", 3)
    .getOrCreate()

  //1. read from the stream
  val linesDf = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "1234")
    .load()

  //2. process
  val wordsDf = linesDf.selectExpr("explode (split(value,' ')) as word")
  val countsDf =   wordsDf.groupBy("word").count()

  //3. write to the sink
  val wordCountQuery = countsDf.writeStream
    .format(source = "console")
    .outputMode(outputMode = "complete")
    .option("checkpointLocation", "checkpoint-location1")
    .start()

  wordCountQuery.awaitTermination()



}