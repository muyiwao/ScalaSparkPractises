import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSqlHive extends App {
  Logger.getLogger("org").setLevel (Level.ERROR)

  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "Group1")
  sparkconf.set("spark.master", "local[*]")

  //For Windows
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession.builder()
    .config(sparkconf)
    .enableHiveSupport()
    .getOrCreate()

  val schema = "date STRING, delay INT, distance INT, origin STRING, destination, STRING"

  val dataDF = (spark.read.format("csv")
    //.option("inferSchema", "true")
    .option(schema, true)
    .option("header", "true")
    .load("departuredelays.csv"))

  //dataDF.show()
  dataDF.createOrReplaceTempView("us_delay_flights_tbl")

  //all flights whose distance is greater than 1,000 miles:
  val res = spark.sql("""SELECT distance, origin, destination
  FROM us_delay_flights_tbl WHERE distance > 1000
  ORDER BY distance DESC""")

  res.show(10)

  res.write.format("csv").save("hiveoutput")


}


/*

+--------+------+-----------+
|distance|origin|destination|
+--------+------+-----------+
|    4330|   HNL|        JFK|
|    4330|   HNL|        JFK|
|    4330|   HNL|        JFK|
|    4330|   HNL|        JFK|
|    4330|   HNL|        JFK|
|    4330|   HNL|        JFK|
|    4330|   HNL|        JFK|
|    4330|   HNL|        JFK|
|    4330|   HNL|        JFK|
|    4330|   HNL|        JFK|
+--------+------+-----------+
only showing top 10 rows


Process finished with exit code 0

 */