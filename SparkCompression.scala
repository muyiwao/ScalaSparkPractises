import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object SparkCompression extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark: SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("AgonMuyiwa")
    .getOrCreate()
  val sc = spark.sparkContext
  
  //val df = spark.read.schema(schema).csv("AM.csv")
  import org.apache.spark.util.SizeEstimator

  val df = spark.read.option("header", true)
    .option("inferSchema", true) // not recommended in production
    .csv(path = "fraud.csv")

  //dataDF.show()
  df.createOrReplaceTempView("FraudTrans")

  //Process data where customer state is Texas TX
  val res = spark.sql(
    """SELECT *
      |FROM FraudTrans
      |WHERE category IS NOT NULL""".stripMargin)

  val avroPath: String = "avro_output"
  //Write avro file
  res.write.format("com.databricks.spark.avro").save(avroPath)

  val avroSize: Float = SizeEstimator.estimate(avroPath)
  println("Avro File Size : " + avroSize + "B")

  // Read the avro file
  //val am_avro = spark.read.format("avro").load("am_avro.avro")
  val am_avro = spark.read.format("avro").load(avroPath)
  //print the data schema
  am_avro.printSchema()
  //print the number of rows
  println(am_avro.count())
  //Save the dataset using different compression types
  am_avro.show(5 )

  //Write the file as parquet using NO COMPRESSION, GZIP, SNAPPY, LZO, BZIP2
  //Target Folder
  var targetFileFolder: String = "compressed_output"

  // Setup theFile Path & File Name
  val lzoPath: String = targetFileFolder + "/am_lzo"
  val gzipPath: String = targetFileFolder + "/am_gzip"
  val snappyPath: String = targetFileFolder + "/am_snappy"
  val uncompressedPath: String = targetFileFolder + "/am_uncompress"

  println("--------------COMPRESSION-----------------")
  //Write the file as parquet using  GZIP
  var t1 = System.nanoTime
  am_avro.write.mode("overwrite").option("compression", "GZIP").save(gzipPath)
  var duration = (System.nanoTime - t1) / 1e9d
  val gzipSize: Float = SizeEstimator.estimate(gzipPath)
  println("GZIP File Size & Time : " + gzipSize + "B" + " (" + duration + ")")


  t1 = System.nanoTime
  am_avro.write.mode("overwrite").option("compression", "lz4").save(lzoPath)
  duration = (System.nanoTime - t1) / 1e9d
  val lzoSize: Float = SizeEstimator.estimate(lzoPath)
  println("Lzo File Size & Time : " + lzoSize + "B" + " (" + duration + ")")


  t1 = System.nanoTime
  am_avro.write.mode("overwrite").option("compression", "snappy").save(snappyPath)
  duration = (System.nanoTime - t1) / 1e9d
  val snappySize: Float = SizeEstimator.estimate(snappyPath)
  println("Snappy File Size & Time : " + snappySize + "B" + " (" + duration + ")")

  t1 = System.nanoTime
  am_avro.write.mode("overwrite").option("compression", "uncompressed").save(uncompressedPath)
  duration = (System.nanoTime - t1) / 1e9d
  val uncompressedSize: Float = SizeEstimator.estimate(uncompressedPath)
  println("Uncompressed File Size & Time : " + uncompressedSize + "B" + " (" + duration + ")")


  //Now let's compare the size of different files
  println("\n---------------------------------------DECOMPRESSION-------------------------------------")
  targetFileFolder = "uncompressed_output"

  // Uncompress the compressed file
  t1 = System.nanoTime
  val df_gzip = spark.read.option("header", true).option("inferSchema", true)
                  .parquet(gzipPath)
  val DecompressedGzipPath: String = targetFileFolder + "/am_decompressed_gzip"
  df_gzip.write.format("com.databricks.spark.avro").save(DecompressedGzipPath)
  duration = (System.nanoTime - t1) / 1e9d
  val DecompressedGzipSize: Float = SizeEstimator.estimate(DecompressedGzipPath)
  println("GZIP File Size & Time : " + DecompressedGzipSize + "B" + " (" + duration + ")")


  t1 = System.nanoTime
  val df_snappy = spark.read.option("header", true).option("inferSchema", true)
    .parquet(snappyPath)
  val DecompressedSnappyPath: String = targetFileFolder + "/am_decompressed_snappy"
  df_snappy.write.format("com.databricks.spark.avro").save(DecompressedSnappyPath)
  duration = (System.nanoTime - t1) / 1e9d
  val DecompressedSnappySize: Float = SizeEstimator.estimate(DecompressedSnappyPath)
  println("Snappy File Size & Time : " + DecompressedSnappySize + "B" + " (" + duration + ")")


  t1 = System.nanoTime
  val df_lzo = spark.read.option("header", true).option("inferSchema", true)
    .parquet(lzoPath)
  val DecompressedLzoPath: String = targetFileFolder + "/am_decompressed_lzo"
  df_lzo.write.format("com.databricks.spark.avro").save(DecompressedLzoPath)
  duration = (System.nanoTime - t1) / 1e9d
  val DecompressedLzoSize: Float = SizeEstimator.estimate(DecompressedLzoPath)
  println("Lzo File Size & Time : " + DecompressedLzoSize + "B" + " (" + duration + ")")


  t1 = System.nanoTime
  val df_uncompressed = spark.read.option("header", true).option("inferSchema", true)
    .parquet(snappyPath)
  val DecompressedUncompressedPath: String = targetFileFolder + "/am_decompressed_uncompressed"
  df_uncompressed.write.format("com.databricks.spark.avro").save(DecompressedUncompressedPath)
  duration = (System.nanoTime - t1) / 1e9d
  val DecompressedUncompressedSize: Float = SizeEstimator.estimate(DecompressedUncompressedPath)
  println("Uncompressed File Size & Time : " + DecompressedUncompressedSize + "B" + " (" + duration + ")")

  //view one of the decompressed files, for example decompressed snappy file
  df_snappy.show(5)
}


/*

Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Avro File Size : 64.0B
root
 |-- _c0: integer (nullable = true)
 |-- trans_date_trans_time: timestamp (nullable = true)
 |-- cc_num: long (nullable = true)
 |-- merchant: string (nullable = true)
 |-- category: string (nullable = true)
 |-- amt: double (nullable = true)
 |-- first: string (nullable = true)
 |-- last: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- street: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- zip: integer (nullable = true)
 |-- lat: double (nullable = true)
 |-- long: double (nullable = true)
 |-- city_pop: integer (nullable = true)
 |-- job: string (nullable = true)
 |-- dob: timestamp (nullable = true)
 |-- trans_num: string (nullable = true)
 |-- unix_time: integer (nullable = true)
 |-- merch_lat: double (nullable = true)
 |-- merch_long: double (nullable = true)
 |-- is_fraud: integer (nullable = true)

1296675
+---+---------------------+----------------+--------------------+-------------+------+---------+-------+------+--------------------+--------------+-----+-----+-------+---------+--------+--------------------+-------------------+--------------------+----------+------------------+-----------+--------+
|_c0|trans_date_trans_time|          cc_num|            merchant|     category|   amt|    first|   last|gender|              street|          city|state|  zip|    lat|     long|city_pop|                 job|                dob|           trans_num| unix_time|         merch_lat| merch_long|is_fraud|
+---+---------------------+----------------+--------------------+-------------+------+---------+-------+------+--------------------+--------------+-----+-----+-------+---------+--------+--------------------+-------------------+--------------------+----------+------------------+-----------+--------+
|  0|  2019-01-01 00:00:18|2703186189652095|fraud_Rippin, Kub...|     misc_net|  4.97| Jennifer|  Banks|     F|      561 Perry Cove|Moravian Falls|   NC|28654|36.0788| -81.1781|    3495|Psychologist, cou...|1988-03-09 00:00:00|0b242abb623afc578...|1325376018|         36.011293| -82.048315|       0|
|  1|  2019-01-01 00:00:44|    630423337322|fraud_Heller, Gut...|  grocery_pos|107.23|Stephanie|   Gill|     F|43039 Riley Green...|        Orient|   WA|99160|48.8878|-118.2105|     149|Special education...|1978-06-21 00:00:00|1f76529f857473494...|1325376044|49.159046999999994|-118.186462|       0|
|  2|  2019-01-01 00:00:51|  38859492057661|fraud_Lind-Buckridge|entertainment|220.11|   Edward|Sanchez|     M|594 White Dale Su...|    Malad City|   ID|83252|42.1808| -112.262|    4154|Nature conservati...|1962-01-19 00:00:00|a1a22d70485983eac...|1325376051|         43.150704|-112.154481|       0|
|  3|  2019-01-01 00:01:16|3534093764340240|fraud_Kutch, Herm...|gas_transport|  45.0|   Jeremy|  White|     M|9443 Cynthia Cour...|       Boulder|   MT|59632|46.2306|-112.1138|    1939|     Patent attorney|1967-01-12 00:00:00|6b849c168bdad6f86...|1325376076|         47.034331|-112.561071|       0|
|  4|  2019-01-01 00:03:06| 375534208663984| fraud_Keeling-Crist|     misc_pos| 41.96|    Tyler| Garcia|     M|    408 Bradley Rest|      Doe Hill|   VA|24433|38.4207| -79.4629|      99|Dance movement ps...|1986-03-28 00:00:00|a41d7549acf907893...|1325376186|         38.674999| -78.632459|       0|
+---+---------------------+----------------+--------------------+-------------+------+---------+-------+------+--------------------+--------------+-----+-----+-------+---------+--------+--------------------+-------------------+--------------------+----------+------------------+-----------+--------+
only showing top 5 rows

--------------COMPRESSION-----------------
GZIP File Size & Time : 96.0B (9.7686967)
Lzo File Size & Time : 88.0B (4.987379)
Snappy File Size & Time : 96.0B (5.504235)
Uncompressed File Size & Time : 104.0B (4.3813002)

---------------------------------------DECOMPRESSION-------------------------------------
GZIP File Size & Time : 120.0B (7.8515302)
Snappy File Size & Time : 128.0B (7.3338308)
Lzo File Size & Time : 120.0B (7.3505494)
Uncompressed File Size & Time : 136.0B (8.9727377)
+---+---------------------+----------------+--------------------+-------------+------+---------+-------+------+--------------------+--------------+-----+-----+-------+---------+--------+--------------------+-------------------+--------------------+----------+------------------+-----------+--------+
|_c0|trans_date_trans_time|          cc_num|            merchant|     category|   amt|    first|   last|gender|              street|          city|state|  zip|    lat|     long|city_pop|                 job|                dob|           trans_num| unix_time|         merch_lat| merch_long|is_fraud|
+---+---------------------+----------------+--------------------+-------------+------+---------+-------+------+--------------------+--------------+-----+-----+-------+---------+--------+--------------------+-------------------+--------------------+----------+------------------+-----------+--------+
|  0|  2019-01-01 00:00:18|2703186189652095|fraud_Rippin, Kub...|     misc_net|  4.97| Jennifer|  Banks|     F|      561 Perry Cove|Moravian Falls|   NC|28654|36.0788| -81.1781|    3495|Psychologist, cou...|1988-03-09 00:00:00|0b242abb623afc578...|1325376018|         36.011293| -82.048315|       0|
|  1|  2019-01-01 00:00:44|    630423337322|fraud_Heller, Gut...|  grocery_pos|107.23|Stephanie|   Gill|     F|43039 Riley Green...|        Orient|   WA|99160|48.8878|-118.2105|     149|Special education...|1978-06-21 00:00:00|1f76529f857473494...|1325376044|49.159046999999994|-118.186462|       0|
|  2|  2019-01-01 00:00:51|  38859492057661|fraud_Lind-Buckridge|entertainment|220.11|   Edward|Sanchez|     M|594 White Dale Su...|    Malad City|   ID|83252|42.1808| -112.262|    4154|Nature conservati...|1962-01-19 00:00:00|a1a22d70485983eac...|1325376051|         43.150704|-112.154481|       0|
|  3|  2019-01-01 00:01:16|3534093764340240|fraud_Kutch, Herm...|gas_transport|  45.0|   Jeremy|  White|     M|9443 Cynthia Cour...|       Boulder|   MT|59632|46.2306|-112.1138|    1939|     Patent attorney|1967-01-12 00:00:00|6b849c168bdad6f86...|1325376076|         47.034331|-112.561071|       0|
|  4|  2019-01-01 00:03:06| 375534208663984| fraud_Keeling-Crist|     misc_pos| 41.96|    Tyler| Garcia|     M|    408 Bradley Rest|      Doe Hill|   VA|24433|38.4207| -79.4629|      99|Dance movement ps...|1986-03-28 00:00:00|a41d7549acf907893...|1325376186|         38.674999| -78.632459|       0|
+---+---------------------+----------------+--------------------+-------------+------+---------+-------+------+--------------------+--------------+-----+-----+-------+---------+--------+--------------------+-------------------+--------------------+----------+------------------+-----------+--------+
only showing top 5 rows


Process finished with exit code 0


 */