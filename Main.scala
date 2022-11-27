import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}


object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "wordcnt")
    //read from file
    val rdd1 = sc.textFile("C:/Demos/input/data.txt")

    /*
    //using anonymous holder
    //one input row will give multiple output rows
    val rdd2 = rdd1.flatMap(x => x.split(" "))
    val rdd21 = rdd2.map(x => x.toLowerCase())
    //one input row will give one output row only
    val rdd3 = rdd21.map(x => (x, 1))
    //take two rows , and does aggregation and returns one row
    val rdd4 = rdd3.reduceByKey((x, y) => x + y)
    // print the output
    rdd4.collect.foreach(println)*/

    // Straight line code
    val rdd2 = rdd1.flatMap(x => x.split(" ")).map(x => x.toLowerCase()).map(x => (x, 1)).reduceByKey((x, y) => x + y)
    //rdd2.collect.foreach(println)

    val res = rdd2.collect
    for (r <- res) {
      val word = r._1
      val count = r._2
      println(s"$word: $count")
    }

    rdd2.saveAsTextFile("C:\\Demos\\output")

    //scala.io.StdIn.readLine()
  }
}