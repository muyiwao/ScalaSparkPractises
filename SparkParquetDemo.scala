import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkParquetDemo extends App {
  Logger.getLogger("org").setLevel (Level.ERROR)

  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "DFDemo")
  sparkconf.set("spark.master", "local[*]")
  val spark = SparkSession.builder().config(sparkconf).getOrCreate()

  val parqDF = spark.read.parquet("zipcodes.parquet")

  parqDF.createOrReplaceTempView("ParquetTable")

  // Join using Spark-Sql table
  val res = spark.sql("select * from ParquetTable " )

  res.show()


}