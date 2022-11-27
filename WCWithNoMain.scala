import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}


object WCWithNoMain extends App {
  //def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "wordcnt")
    //read from file
    val rdd1 = sc.textFile("C:/Demos/input/data.txt")


    // Straight line code
    val rdd2 = rdd1.flatMap(x => x.split(" ")).map(x => x.toLowerCase()).map(x => (x, 1)).reduceByKey((x, y) => x + y)
    //rdd2.collect.foreach(println)

    val res = rdd2.collect
    for (r <- res) {
      val word = r._1
      val count = r._2
      println(s"$word: $count")
    }


    //scala.io.StdIn.readLine()
  //}
}
