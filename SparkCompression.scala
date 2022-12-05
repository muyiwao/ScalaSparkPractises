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

  //read json file into dataframe
  val df = spark.read.csv("AM.csv")

  //Define custom schema
  val schema = new StructType()
    .add("customer_id", IntegerType, true)
    .add("customer_fname", StringType, true)
    .add("customer_lname", StringType, true)
    .add("customer_email", StringType, true)
    .add("customer_password", StringType, true)
    .add("customer_street", StringType, true)
    .add("customer_city", StringType, true)
    .add("customer_state", StringType, true)
    .add("customer_zipcode", IntegerType, true)


  val df_with_schema = spark.read.schema(schema).csv("AM.csv")

  //dataDF.show()
  df_with_schema.createOrReplaceTempView("AM")

  //Process data where customer state is Texas TX
  val res = spark.sql(
    """SELECT *
      |FROM AM
      |WHERE customer_state IS NOT NULL""".stripMargin)

  //res.show()

  //Write avro file
  res.write.format("com.databricks.spark.avro").save("avro_output")

  val avroPath : String = "avro_output"
  val avroSize: Int = avroPath.length
  println("Avro File Size : " + avroSize)

  // Read the avro file
  //val am_avro = spark.read.format("avro").load("am_avro.avro")
  val am_avro = spark.read.format("avro").load(avroPath)
  println(am_avro.count())

  //Save the dataset using different compression types
  am_avro.show()

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
  val gzipSize: Int = gzipPath.length
  println("GZIP File Size & Time : " + gzipSize + " (" + duration + ")")


  t1 = System.nanoTime
  am_avro.write.mode("overwrite").option("compression", "lz4").save(lzoPath)
  duration = (System.nanoTime - t1) / 1e9d
  val lzoSize: Int = lzoPath.length
  println("Lzo File Size & Time : " + lzoSize + " (" + duration + ")")


  t1 = System.nanoTime
  am_avro.write.mode("overwrite").option("compression", "snappy").save(snappyPath)
  duration = (System.nanoTime - t1) / 1e9d
  val snappySize: Int = snappyPath.length
  println("Snappy File Size & Time : " + snappySize + " (" + duration + ")")


  t1 = System.nanoTime
  am_avro.write.mode("overwrite").option("compression", "uncompressed").save(uncompressedPath)
  duration = (System.nanoTime - t1) / 1e9d
  val uncompressedSize: Int = uncompressedPath.length
  println("Uncompressed File Size & Time : " + uncompressedSize + " (" + duration + ")")


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
  val DecompressedGzipSize: Int = DecompressedGzipPath.length
  println("GZIP File Size & Time : " + DecompressedGzipSize + " (" + duration + ")")


  t1 = System.nanoTime
  val df_snappy = spark.read.option("header", true).option("inferSchema", true)
    .parquet(snappyPath)
  val DecompressedSnappyPath: String = targetFileFolder + "/am_decompressed_snappy"
  df_snappy.write.format("com.databricks.spark.avro").save(DecompressedSnappyPath)
  duration = (System.nanoTime - t1) / 1e9d
  val DecompressedSnappySize: Int = DecompressedSnappyPath.length
  println("Snappy File Size & Time : " + DecompressedSnappySize + " (" + duration + ")")


  t1 = System.nanoTime
  val df_lzo = spark.read.option("header", true).option("inferSchema", true)
    .parquet(lzoPath)
  val DecompressedLzoPath: String = targetFileFolder + "/am_decompressed_lzo"
  df_lzo.write.format("com.databricks.spark.avro").save(DecompressedLzoPath)
  duration = (System.nanoTime - t1) / 1e9d
  val DecompressedLzoSize: Int = DecompressedLzoPath.length
  println("Lzo File Size & Time : " + DecompressedLzoSize + " (" + duration + ")")


  t1 = System.nanoTime
  val df_uncompressed = spark.read.option("header", true).option("inferSchema", true)
    .parquet(snappyPath)
  val DecompressedUncompressedPath: String = targetFileFolder + "/am_decompressed_uncompressed"
  df_uncompressed.write.format("com.databricks.spark.avro").save(DecompressedUncompressedPath)
  duration = (System.nanoTime - t1) / 1e9d
  val DecompressedUncompressedSize: Int = DecompressedUncompressedPath.length
  println("Uncompressed File Size & Time : " + DecompressedUncompressedSize + " (" + duration + ")")

}


/*

Avro File Size : 11
12435
+-----------+--------------+--------------+--------------+-----------------+--------------------+-------------+--------------+----------------+
|customer_id|customer_fname|customer_lname|customer_email|customer_password|     customer_street|customer_city|customer_state|customer_zipcode|
+-----------+--------------+--------------+--------------+-----------------+--------------------+-------------+--------------+----------------+
|          1|       Richard|     Hernandez|     XXXXXXXXX|        XXXXXXXXX|  6303 Heather Plaza|  Brownsville|            TX|           78521|
|          2|          Mary|       Barrett|     XXXXXXXXX|        XXXXXXXXX|9526 Noble Embers...|    Littleton|            CO|           80126|
|          3|           Ann|         Smith|     XXXXXXXXX|        XXXXXXXXX|3422 Blue Pioneer...|       Caguas|            PR|             725|
|          4|          Mary|         Jones|     XXXXXXXXX|        XXXXXXXXX|  8324 Little Common|   San Marcos|            CA|           92069|
|          5|        Robert|        Hudson|     XXXXXXXXX|        XXXXXXXXX|10 Crystal River ...|       Caguas|            PR|             725|
|          6|          Mary|         Smith|     XXXXXXXXX|        XXXXXXXXX|3151 Sleepy Quail...|      Passaic|            NJ|            7055|
|          7|       Melissa|        Wilcox|     XXXXXXXXX|        XXXXXXXXX|9453 High Concession|       Caguas|            PR|             725|
|          8|         Megan|         Smith|     XXXXXXXXX|        XXXXXXXXX|3047 Foggy Forest...|     Lawrence|            MA|            1841|
|          9|          Mary|         Perez|     XXXXXXXXX|        XXXXXXXXX| 3616 Quaking Street|       Caguas|            PR|             725|
|         10|       Melissa|         Smith|     XXXXXXXXX|        XXXXXXXXX|8598 Harvest Beac...|     Stafford|            VA|           22554|
|         11|          Mary|       Huffman|     XXXXXXXXX|        XXXXXXXXX|    3169 Stony Woods|       Caguas|            PR|             725|
|         12|   Christopher|         Smith|     XXXXXXXXX|        XXXXXXXXX|5594 Jagged Ember...|  San Antonio|            TX|           78227|
|         13|          Mary|       Baldwin|     XXXXXXXXX|        XXXXXXXXX|7922 Iron Oak Gar...|       Caguas|            PR|             725|
|         14|     Katherine|         Smith|     XXXXXXXXX|        XXXXXXXXX|5666 Hazy Pony Sq...|  Pico Rivera|            CA|           90660|
|         15|          Jane|          Luna|     XXXXXXXXX|        XXXXXXXXX|    673 Burning Glen|      Fontana|            CA|           92336|
|         16|       Tiffany|         Smith|     XXXXXXXXX|        XXXXXXXXX|      6651 Iron Port|       Caguas|            PR|             725|
|         17|          Mary|      Robinson|     XXXXXXXXX|        XXXXXXXXX|     1325 Noble Pike|       Taylor|            MI|           48180|
|         18|        Robert|         Smith|     XXXXXXXXX|        XXXXXXXXX|2734 Hazy Butterf...|     Martinez|            CA|           94553|
|         19|     Stephanie|      Mitchell|     XXXXXXXXX|        XXXXXXXXX|3543 Red Treasure...|       Caguas|            PR|             725|
|         20|          Mary|         Ellis|     XXXXXXXXX|        XXXXXXXXX|      4703 Old Route|West New York|            NJ|            7093|
+-----------+--------------+--------------+--------------+-----------------+--------------------+-------------+--------------+----------------+
only showing top 20 rows

--------------COMPRESSION-----------------
GZIP File Size & Time : 25 (1.0195129)
Lzo File Size & Time : 24 (0.3302963)
Snappy File Size & Time : 27 (0.261351)
Uncompressed File Size & Time : 31 (0.2556255)

---------------------------------------DECOMPRESSION-------------------------------------
GZIP File Size & Time : 40 (0.5191918)
Snappy File Size & Time : 42 (0.3519731)
Lzo File Size & Time : 39 (0.3420243)
Uncompressed File Size & Time : 48 (0.296975)

Process finished with exit code 0

 */