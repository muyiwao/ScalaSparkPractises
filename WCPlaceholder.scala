import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}


object WCPlaceholder {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "wordcnt")
    //read from file
    val rdd1 = sc.textFile("C:/Demos/input/data.txt")

    /*
    //using anonymous holder
    //one input row will give multiple output rows
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd21 = rdd2.map(_.toLowerCase())
    //one input row will give one output row only
    val rdd3 = rdd21.map((_, 1))
    //take two rows , and does aggregation and returns one row
    val rdd4 = rdd3.reduceByKey(_ + _)
    // print the output
    rdd4.collect.foreach(println)
    */

    val rdd2 = rdd1.flatMap(_.split(" ")).map(_.toLowerCase()).map((_, 1)).reduceByKey(_ + _)
    rdd2.collect.foreach(println)

    //scala.io.StdIn.readLine()
  }
}
