import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object dataExploration extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .master(master = "local[2]")
    .appName(name = "Data Exploration") //.config("spark.sql.shuffle.partitions", 3)
    .getOrCreate()

  /**
   * Load and prepare data
   */

  val rawData = spark.read
    .format("csv")
    .option("comment", "#")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("fraud.csv")

  val cols = rawData.columns
  val labelCol = cols.last

  rawData.show(5)
  rawData.printSchema()

  rawData.createOrReplaceTempView("creditcardfraud")

  println("//get a sense of the distribution of the data")
  val sqlDF = spark.sql("SELECT COUNT(*) FROM creditcardfraud")
  sqlDF.show()

  println("//check the ratio of fraudulent vs. legitimate transactions")
  val sqlDF2 = spark.sql(
    """SELECT is_fraud, COUNT(is_fraud) AS CASE_COUNT
      |FROM creditcardfraud
      |GROUP BY is_fraud""".stripMargin)
  sqlDF2.show()

  println("//check the fraudulent vs. legitimate transactions based gender columns")
  val sqlDF3 = spark.sql(
    """SELECT gender, is_fraud, COUNT(is_fraud) AS CASE_COUNT
      |FROM creditcardfraud
      |GROUP BY gender, is_fraud
      |SORT BY CASE_COUNT DESC""".stripMargin)
  sqlDF3.show()



}


