import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}


object WCCountByKey {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "wordcnt")
    //read from file
    val rdd1 = sc.textFile("C:/Demos/input/data.txt")

    val rdd2 = rdd1.flatMap(x => x.split(" "))
                   .map(x => x.toLowerCase())
                   .map(x => (x, 1))
                   .countByKey()

    println(rdd2)
    //rdd2.collect.foreach(println)

    //scala.io.StdIn.readLine()
  }
}
