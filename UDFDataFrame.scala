import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UDFDataFrame extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[3]") //"local[3]"
    .appName("SparkByExample") //
    .getOrCreate()

  val data = Seq(("2018/01/23", 23), ("2018/01/24", 24), ("2018/02/20", 25))

  import spark.sqlContext.implicits._
  val df = data.toDF("date1", "day")

  df.show();

  val replace: String => String = _.replace("/", "-")

  import org.apache.spark.sql.functions._ // _ instead of udf

  val replaceUDF = udf(replace) // kEY PART

  val minDate = df.agg(min($"date1")).collect()(0).get(0)

  val df2 = df.select("*").filter(to_date(replaceUDF($"date1")) > date_add(to_date(replaceUDF(lit(minDate))), 7))

  df2.show()
  spark.close()



}
