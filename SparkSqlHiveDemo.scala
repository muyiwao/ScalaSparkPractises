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
    //.master("local")
    //.master("local[*]")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  val schema = "date STRING, delay INT, distance INT, origin STRING, destination, STRING"

  val dataDF = (spark.read.format("csv")
    //.option("inferSchema", "true")
    .option(schema, true)
    .option("header", "true")
    .load("departuredelays.csv"))

  dataDF.printSchema()

  //dataDF.show()
  dataDF.createOrReplaceTempView("us_delay_flights_tbl")

  val record_counts = spark.sql("SELECT count(*) FROM us_delay_flights_tbl")
  record_counts.show()

  //All flights whose distance is greater than 1,000 miles:
  val res = spark.sql("""SELECT date, distance, origin, destination
  FROM us_delay_flights_tbl WHERE distance > 1000
  ORDER BY distance DESC""")
  res.show(10)

  // save the output
  //res.write.format("csv").mode("overwrite").save("flightCsv")


  //Save DataFrame as a new Hive table Use the following code
  // to save the data frame to a new hive table named flight_table1:
  // Save df to a new table in Hive
  // https://kontext.tech/article/294/spark-save-dataframe-to-hive-table

  res.write.mode("overwrite").saveAsTable("flight_table1")

  //Show the results using SELECT
  spark.sql("select * from flight_table1").show(5)

}

/*
Output:
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
root
 |-- date: string (nullable = true)
 |-- delay: string (nullable = true)
 |-- distance: string (nullable = true)
 |-- origin: string (nullable = true)
 |-- destination: string (nullable = true)

22/12/18 20:01:19 INFO Persistence: Property hive.metastore.integral.jdo.pushdown unknown - will be ignored
22/12/18 20:01:19 INFO Persistence: Property datanucleus.cache.level2 unknown - will be ignored
22/12/18 20:01:20 INFO Datastore: The class "org.apache.hadoop.hive.metastore.model.MFieldSchema" is tagged as "embedded-only" so does not have its own datastore table.
22/12/18 20:01:20 INFO Datastore: The class "org.apache.hadoop.hive.metastore.model.MOrder" is tagged as "embedded-only" so does not have its own datastore table.
22/12/18 20:01:20 INFO Datastore: The class "org.apache.hadoop.hive.metastore.model.MFieldSchema" is tagged as "embedded-only" so does not have its own datastore table.
22/12/18 20:01:20 INFO Datastore: The class "org.apache.hadoop.hive.metastore.model.MOrder" is tagged as "embedded-only" so does not have its own datastore table.
22/12/18 20:01:20 INFO Query: Reading in results for query "org.datanucleus.store.rdbms.query.SQLQuery@0" since the connection used is closing
22/12/18 20:01:21 INFO Datastore: The class "org.apache.hadoop.hive.metastore.model.MResourceUri" is tagged as "embedded-only" so does not have its own datastore table.
+--------+
|count(1)|
+--------+
| 1391578|
+--------+

+--------+--------+------+-----------+
|    date|distance|origin|destination|
+--------+--------+------+-----------+
|01091625|    4330|   HNL|        JFK|
|01051625|    4330|   HNL|        JFK|
|01081625|    4330|   HNL|        JFK|
|01021625|    4330|   HNL|        JFK|
|01041625|    4330|   HNL|        JFK|
|01061625|    4330|   HNL|        JFK|
|01071625|    4330|   HNL|        JFK|
|01011625|    4330|   HNL|        JFK|
|01111625|    4330|   HNL|        JFK|
|01031625|    4330|   HNL|        JFK|
+--------+--------+------+-----------+
only showing top 10 rows

22/12/18 20:01:29 INFO log: Updating table stats fast for flight_table1
22/12/18 20:01:29 INFO log: Updated size of table flight_table1 to 1374688
+--------+--------+------+-----------+
|    date|distance|origin|destination|
+--------+--------+------+-----------+
|01010900|    2151|   JFK|        LAX|
|01011200|    2151|   JFK|        LAX|
|01011900|    2151|   JFK|        LAX|
|01011345|    2151|   JFK|        LAX|
|01011545|    2151|   JFK|        LAX|
+--------+--------+------+-----------+
only showing top 5 rows


Process finished with exit code 0

 */

