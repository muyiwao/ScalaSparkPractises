import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SqlScalaHiveDemo extends App {
  Logger.getLogger("org").setLevel (Level.ERROR)

  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "Group1")
  sparkconf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkconf).getOrCreate()

  val dataDF = spark.read.option("header", true)
    .option("inferSchema", true)
    .csv("grptable.csv")

  //dataDF.show()
  dataDF.createOrReplaceTempView("tableDataName")

  val res = spark.sql("select * from tableDataName where age = 25")

  res.write.saveAsTable("groupdata")

  res.show()


}
