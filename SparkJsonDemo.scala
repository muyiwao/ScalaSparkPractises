import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkJsonDemo extends App {
  Logger.getLogger("org").setLevel (Level.ERROR)

  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "SparkExamples")
  sparkconf.set("spark.master", "local[*]")
  val spark = SparkSession.builder().config(sparkconf).getOrCreate()

  val zipCodeDF = spark.read.json("zipcodes.json")
  zipCodeDF.printSchema()
  zipCodeDF.show(false)

  //read multiline json file
  val multiline_ZipCodeDF = spark.read.option("multiline", "true").json("zipcodes.json")
  multiline_ZipCodeDF.printSchema()
  multiline_ZipCodeDF.show(false)

}
