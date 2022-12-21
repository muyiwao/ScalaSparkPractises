/**
 * Start by importing the necessary libraries for Kafka streaming,
 * JSON processing, and model saving
 */

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSourceOffset.format
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Duration


object KafkaStreamPredictor extends App {
    val spark = SparkSession.builder()
      .appName("KafkaStreamPredictor")
      .getOrCreate()

  /**
   * Set up a Spark Streaming context and a Kafka stream.
   * Specify the Kafka broker address, the topic to stream from, and the number of threads to use.
   */

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Duration(10)) // batch interval of 10 seconds

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "groupId",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("topic")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

  /**
   * Load the saved model from a file.load method of the model class to load the model
   * from a file in the local filesystem
   */
    val model = PipelineModel.load("/output/LR_model")

  /**
   * Process the data from the stream.
   * Use map function to transform each record in the stream into a JSON object,
   * and then extract the relevant data from the JSON
   */

    val data = stream.map { record =>
      val json = parse(record.value())
      val features = (json \ "features").extract[Seq[Double]]
      features
    }

  /**
   * Use the model to make predictions on the processed data.
   * Use transform method of the model to make predictions and extract the prediction column.
   */

    val predictions = model.transform(data)
      .select("prediction")

  /**
   * Save the predictions to a file or perform some other action with the predictions.
   * Write the predictions to a Kafka topic or save them to a file in the local filesystem
   */

    predictions.write.json("output/predictions")

  /**
   * Start the streaming context and wait for the stream to end.
   */
    ssc.start()
    ssc.awaitTermination()
}

