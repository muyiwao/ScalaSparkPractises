import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._


object rddToDF extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "App")
  sparkconf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkconf).getOrCreate()

  val myList = List(
    (1, "2013-07-25", 11599, "closed"),
    (2, "2013-07-26", 11599, "closed"),
    (3, "2013-07-27", 11599, "closed"),
    (4, "2013-07-25", 11599, "closed")
  )
  // convert rdd t df
  import spark.implicits._
  //way 1
  val rdd= spark.sparkContext.parallelize(myList)
  rdd.toDF()

  //way 2
  val orderDF = spark.createDataFrame(myList) // convert to rdd
    .toDF("o_id","orderdate","cid","status") // add headers
    .withColumn("newid", monotonically_increasing_id()) //add columns with unique id
    .withColumn("date1", unix_timestamp(col("orderdate").cast(DateType))) // add column to change date format
    .drop("o_id") // delete column
    .dropDuplicates("orderdate") // delete duplicates values
    .sort("orderdate")
    .show()




}
