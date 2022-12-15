import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSqlHiveDemo extends App {
  Logger.getLogger("org").setLevel (Level.ERROR)

  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "Project1")
  sparkconf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkconf)
    .getOrCreate()

  import spark.implicits._

  val schema = "date STRING, delay INT, distance INT, origin STRING, destination, STRING"

  val dataDF = (spark.read.format("csv")
    //.option("inferSchema", "true")
    .option(schema, true)
    .option("header", "true")
    .load("departuredelays.csv"))

  //dataDF.show()
  dataDF.createOrReplaceTempView("us_delay_flights_tbl")

  val record_counts = spark.sql("SELECT count(*) FROM us_delay_flights_tbl")
  record_counts.show()

  //All flights whose distance is greater than 1,000 miles:
  val res = spark.sql("""SELECT distance, origin, destination
  FROM us_delay_flights_tbl WHERE distance > 1000
  ORDER BY distance DESC""")
  res.show(10)

  //res.write.option("header",false).csv("output_csv")
  res.write.format("csv").mode("overwrite").save("flightCsv")


  //res.write.mode(SaveMode.Overwrite).csv("csv_output")




}

