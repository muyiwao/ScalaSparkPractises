import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object dataProcessing extends App {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .master(master = "local[2]")
      .appName(name = "Data Processing") //.config("spark.sql.shuffle.partitions", 3)
      .getOrCreate()

    /**
     * Load and prepare data
     */
    val filepath = "input/fraud_batch.json"

    val rawData = spark.read
      .format("json")
      .option("comment", "#")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(filepath)

    val cols = rawData.columns
    val labelCol = cols.last

    //rawData.show(5)
    rawData.printSchema()
    rawData.show(5)

    /**
     * Add distance column (Distance to Merchant). The distance to the merchant may be relevant
     * */

    //https://rosettacode.org/wiki/Haversine_formula#Scala
    import math._
    def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double) = {
      val R = 6372.8 //radius in km
      val dLat = (lat2 - lat1).toRadians
      val dLon = (lon2 - lon1).toRadians

      val a = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(lat1.toRadians) * cos(lat2.toRadians)
      val c: Double = 2 * asin(sqrt(a))
      R * c
    }

    val coder_udf = udf(haversine _)
    spark.udf.register("haversine", haversine _)
    val Df = rawData.withColumn("distance", coder_udf(rawData.col("lat"),
      rawData.col("long"), rawData.col("merch_lat"), rawData.col("merch_long"))
    )

    /**
     * Add day_of_week and week_of_month columns
     * */

    //https://sparkbyexamples.com/spark/spark-get-day-of-week-number/
    val Df1 = Df.withColumn("day_of_week", date_format(col("trans_date_trans_time"), "u").cast("int"))
      .withColumn("week_of_month", date_format(col("trans_date_trans_time"), "W").cast("int"))

    /*---------------------------------------------------------------------------------
    val TransDf = Df1.drop("ssn", "cc_num", "first", "last", "street", "city", "state", "zip", "job", "acct_num",
      "profile", "trans_num", "unix_time", "merchant", "dob", "trans_date", "gender", "_c0",
      "Unnamed: 0", "lat", "long", "city_pop", "merch_lat", "merch_long", "category")

    TransDf.createOrReplaceTempView("creditCardFraudTrans")
    val FraudDf = spark.sql("select * from creditCardFraudTrans")

    FraudDf.write.option("header", true).csv("output/output_csv")
    .................................................................................
    */

    /**
     * Dropping multiple columns
     * */
    val Df2 = Df1.drop("ssn", "cc_num", "first", "last", "street", "city", "state", "zip", "job", "acct_num",
      "profile", "trans_num", "unix_time", "merchant", "dob", "trans_date", "trans_date_trans_time", "gender", "_c0",
      "Unnamed: 0", "lat", "long", "city_pop", "merch_lat", "merch_long", "category")

    /**
     * Rearrange columns
     * */
    // https://stackoverflow.com/questions/42912156/python-pyspark-data-frame-rearrange-columns
    val Df3 = Df2.select("day_of_week", "week_of_month", "amt", "distance", "is_fraud")
    Df3.printSchema()


  /**
   * Persist the trained model on disk
   */
  // You can ensure you don't overwrite an existing model by removing .overwrite from this command
    Df3.write.option("header", true).csv("output/processed_data")

}

/*
Output:
root
 |-- Unnamed: 0: long (nullable = true)
 |-- amt: double (nullable = true)
 |-- category: string (nullable = true)
 |-- cc_num: long (nullable = true)
 |-- city: string (nullable = true)
 |-- city_pop: long (nullable = true)
 |-- dob: string (nullable = true)
 |-- first: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- is_fraud: long (nullable = true)
 |-- job: string (nullable = true)
 |-- last: string (nullable = true)
 |-- lat: double (nullable = true)
 |-- long: double (nullable = true)
 |-- merch_lat: double (nullable = true)
 |-- merch_long: double (nullable = true)
 |-- merchant: string (nullable = true)
 |-- state: string (nullable = true)
 |-- street: string (nullable = true)
 |-- trans_date_trans_time: string (nullable = true)
 |-- trans_num: string (nullable = true)
 |-- unix_time: long (nullable = true)
 |-- zip: long (nullable = true)

+----------+------+------------+-------------------+--------------+--------+----------+-------+------+--------+-------------+--------+-------+---------+---------+-----------+--------------------+-----+--------------------+---------------------+--------------------+----------+-----+
|Unnamed: 0|   amt|    category|             cc_num|          city|city_pop|       dob|  first|gender|is_fraud|          job|    last|    lat|     long|merch_lat| merch_long|            merchant|state|              street|trans_date_trans_time|           trans_num| unix_time|  zip|
+----------+------+------------+-------------------+--------------+--------+----------+-------+------+--------+-------------+--------+-------+---------+---------+-----------+--------------------+-----+--------------------+---------------------+--------------------+----------+-----+
|    364558|125.02| food_dining|   4005676619255478|Denham Springs|   71335|1994-05-31|William|     M|       0|    Herbalist|   Perry| 30.459| -90.9027|30.524001| -90.398352|    fraud_Kuphal-Toy|   LA|458 Phillips Isla...|  2019-06-17 23:22:30|90209c1592a9dcf73...|1339975350|70726|
|    834017|   3.1|      travel|    344342339068828|        Darien|    5989|1967-05-05|   Ruth|     F|       0|  Tax adviser|  Fuller|31.3826| -81.4312|32.030518| -82.329973|fraud_Schroeder, ...|   GA|37732 Joe Courts ...|  2019-12-11 23:22:39|b41a985d35c1a9f29...|1355268159|31305|
|    489882|116.25|shopping_net|   3576431665303017|       Phoenix| 1312922|1981-10-24|Jessica|     F|       0|   Contractor|    Ward|33.5623|-112.0559|33.316304|-112.254863|   fraud_Cormier LLC|   AZ|72269 Elizabeth F...|  2019-08-02 12:09:07|e9c33bb5700242473...|1343909347|85020|
|    506493| 49.01| food_dining|4292743669224718067|   Great Mills|    5927|1973-06-09|Michael|     M|       0|Art therapist|Williams|38.2674| -76.4954|38.447952|  -75.99209|fraud_Armstrong, ...|   MD|35822 Clayton Str...|  2019-08-07 23:43:44|27c8badaa6fec360c...|1344383024|20634|
|    668183|158.76| grocery_pos|      4623560839669|     Lohrville|     695|1954-07-15|Vincent|     M|       0|Administrator|  Waller|42.2619| -94.5566| 41.82682|  -93.97377|fraud_Kovacek, Di...|   IA|9379 Vanessa Run ...|  2019-10-12 07:42:58|71568857e3a88baea...|1350027778|51453|
+----------+------+------------+-------------------+--------------+--------+----------+-------+------+--------+-------------+--------+-------+---------+---------+-----------+--------------------+-----+--------------------+---------------------+--------------------+----------+-----+
only showing top 5 rows

root
 |-- day_of_week: integer (nullable = true)
 |-- week_of_month: integer (nullable = true)
 |-- amt: double (nullable = true)
 |-- distance: double (nullable = true)
 |-- is_fraud: long (nullable = true)


Process finished with exit code 0
 */