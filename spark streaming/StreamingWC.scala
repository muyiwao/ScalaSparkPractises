import org.apache.log4j. {Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._

object StreamingWC extends App{
  Logger.getLogger( "org").setLevel (Level.ERROR)
  val sc = new SparkContext ( master = "local[*]", appName = "wordcnt") //creating spark streaming context
  val ssc = new StreamingContext(sc, Seconds(2))
  //lines is a dstream
  val lines = ssc.socketTextStream( hostname = "localhost", port = 9998) //words is a transformed dstream
  val words = lines.flatMap(x => x.split(" "))
  val pairs = words.map(x => (x,1))
  val wordCounts = pairs. reduceByKey((x,y) => x+y)
  wordCounts.print()
  ssc.start()
  ssc.awaitTermination()
  //run producer C:\Program Files (x86)\Nmap>ncat -lvp 9998 //run consumer
}
