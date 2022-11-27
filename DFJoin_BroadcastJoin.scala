import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object DFJoin_BroadcastJoin extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("app")
    .master("local[*]")
    .getOrCreate()


  //val spark = SparkSession.builder().config(sparkconf).getOrCreate()

  import spark.implicits._

  //  creating the employee directory which is bigger dataframe
  val employeeDF = Seq(
    ("Amit", "Bangalore"),
    ("Ankit", "California"),
    ("Abdul", "Pune"),
    ("Sumit", "California"),
    ("Riya", "Pune")
  ).toDF("first_name", "city")

  //  creating the citiesDf which is small df that will be broadcasted
  val citiesDF = Seq(
    ("California", "Usa"),
    ("Bangalore", "india"),
    ("Pune", "India")
  ).toDF("city", "country")


  //  Now we will perform the join operation on employeeDF with broadcasted citiesDF
   var joinedDf = employeeDF.join(broadcast(citiesDF), employeeDF.col("city") === citiesDF.col("city"))

  //  var joinedDf = employeeDF.join(citiesDF, employeeDF.col("city") === citiesDF.col("city"))

  //  Now we will drop the city column from citiesDF as we don't want to keep duplicate column
  joinedDf = joinedDf.drop(citiesDF.col("city"))

  //  Finally we will see the joinedDF
  joinedDf.show()


}
