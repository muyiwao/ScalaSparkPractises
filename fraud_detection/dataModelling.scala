import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object dataModelling extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .master(master = "local[2]")
    .appName(name = "Data Modelling") //.config("spark.sql.shuffle.partitions", 3)
    .getOrCreate()

  import org.apache.spark.ml.classification.LogisticRegression
  import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
  import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
  import org.apache.spark.ml.param.ParamMap
  import spark.sqlContext.implicits._

  /**
   * Load and prepare data
   */
  //https://medium.com/rahasak/logistic-regression-with-apache-spark-b7ec4c98cfcd
  val filepath = "output/processed_data"

  val modelData = spark.read
    .format("csv")
    .option("comment", "#")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filepath)

  modelData.printSchema()
  modelData.show(5)

  // columns that need to added to feature column
  val cols = Array("day_of_week", "week_of_month", "amt", "distance", "is_fraud")


  /**
   * Now comes something more complicated.  Our dataframe has the column headings
   * we created with the schema.  But we need a column called “label” and one called
   * “features” to plug into the LR algorithm.  So we use the VectorAssembler() to do that.
   * Features is a Vector of doubles.  These are all the values like "day_of_week", "week_of_month", "amt", "distance".
   * that we extracted above.  The label indicated whether it's a fraud.
   */

  // VectorAssembler to add feature column
  // input columns - cols
  // feature column - features
  val assembler = new VectorAssembler()
    .setInputCols(cols)
    .setOutputCol("features")

  val featureDf = assembler.transform(modelData)
  featureDf.printSchema()

  featureDf.show(10)


  /**
   * Then we use the StringIndexer to take the column is_fraud and make that the label.
   * is_fraud is the 1 or 0 indicator that shows whether the transaction is a fraud or not.
   * Like the VectorAssembler it will add another column to the dataframe.
   */

  // StringIndexer define new 'label' column with 'result' column
  val indexer = new StringIndexer().setInputCol("is_fraud").setOutputCol("label")
  val labelDf = indexer.fit(featureDf).transform(featureDf)
  labelDf.printSchema()

  labelDf.show(10)


  /** Build Logistic Regression model:
   * In order to train and test the model the data set need to be split
   * into a training data set and a test data set. 70% of the data is used to train the model,
   * and 30% will be used for testing. Like the VectorAssembler it will add another column to the dataframe.
   */

  // split data set training and test
  // training data set - 70%
  // test data set - 30%
  val seed = 5043
  val Array(trainingData, testData) = labelDf.randomSplit(Array(0.7, 0.3), seed)

  // train logistic regression model with training data set
  val logisticRegression = new LogisticRegression()
    .setMaxIter(100)
    .setRegParam(0.02)
    .setElasticNetParam(0.8)
  val logisticRegressionModel = logisticRegression.fit(trainingData)


  // run model with test data set to get predictions
  // this will add new columns rawPrediction, probability and prediction
  val predictionDf = logisticRegressionModel.transform(testData)
  predictionDf.printSchema()
  //predictionDf.show(10)

  predictionDf.createOrReplaceTempView("PredictionTable")

  val fraud_pred = spark.sql("select * from PredictionTable where prediction == 1")
  fraud_pred.show(2)

  val non_fraud_pred = spark.sql("select * from PredictionTable where prediction == 0")
  non_fraud_pred.show(3)

  /** evaluate model with area under ROC:
   * To evaluate the accuracy of a Logistic Regression model is Area Under the ROC Curve(AUC).
   * We can use the BinaryClasssificationEvaluator to obtain the AUC. It required two columns,
   * label and prediction to evaluate the model.and 30% will be used for testing.
   * Like the VectorAssembler it will add another column to the dataframe.
   */

  // evaluate model with area under ROC
  val evaluator = new BinaryClassificationEvaluator()
    .setLabelCol("label")
    .setRawPredictionCol("prediction")
    .setMetricName("areaUnderROC")

  // measure the accuracy
  val accuracy = evaluator.evaluate(predictionDf)
  println("Prediction Accuracy: " + accuracy)

  // Save the model in a path
  logisticRegressionModel.save("output/LR_model")

}


/*
output
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
  root
   |-- day_of_week: integer (nullable = true)
   |-- week_of_month: integer (nullable = true)
   |-- amt: double (nullable = true)
   |-- distance: double (nullable = true)
   |-- is_fraud: integer (nullable = true)

  +-----------+-------------+------+------------------+--------+
  |day_of_week|week_of_month|   amt|          distance|is_fraud|
  +-----------+-------------+------+------------------+--------+
  |          1|            4|125.02|48.876478206769406|       0|
  |          3|            2|   3.1|111.47316521882695|       0|
  |          5|            1|116.25|33.009955560792996|       0|
  |          3|            2| 49.01|48.273248442946404|       0|
  |          6|            2|158.76| 68.25966084141116|       0|
  +-----------+-------------+------+------------------+--------+
  only showing top 5 rows

  root
   |-- day_of_week: integer (nullable = true)
   |-- week_of_month: integer (nullable = true)
   |-- amt: double (nullable = true)
   |-- distance: double (nullable = true)
   |-- is_fraud: integer (nullable = true)
   |-- features: vector (nullable = true)

  +-----------+-------------+------+------------------+--------+--------------------+
  |day_of_week|week_of_month|   amt|          distance|is_fraud|            features|
  +-----------+-------------+------+------------------+--------+--------------------+
  |          1|            4|125.02|48.876478206769406|       0|[1.0,4.0,125.02,4...|
  |          3|            2|   3.1|111.47316521882695|       0|[3.0,2.0,3.1,111....|
  |          5|            1|116.25|33.009955560792996|       0|[5.0,1.0,116.25,3...|
  |          3|            2| 49.01|48.273248442946404|       0|[3.0,2.0,49.01,48...|
  |          6|            2|158.76| 68.25966084141116|       0|[6.0,2.0,158.76,6...|
  |          6|            1| 69.08|63.191419440317354|       0|[6.0,1.0,69.08,63...|
  |          1|            2| 12.08|103.77304260763073|       0|[1.0,2.0,12.08,10...|
  |          1|            5| 20.51| 38.94042126973156|       0|[1.0,5.0,20.51,38...|
  |          7|            3| 16.63| 97.85432735787023|       0|[7.0,3.0,16.63,97...|
  |          2|            4| 43.08| 41.70580386795137|       0|[2.0,4.0,43.08,41...|
  +-----------+-------------+------+------------------+--------+--------------------+
  only showing top 10 rows

  root
   |-- day_of_week: integer (nullable = true)
   |-- week_of_month: integer (nullable = true)
   |-- amt: double (nullable = true)
   |-- distance: double (nullable = true)
   |-- is_fraud: integer (nullable = true)
   |-- features: vector (nullable = true)
   |-- label: double (nullable = false)

  +-----------+-------------+------+------------------+--------+--------------------+-----+
  |day_of_week|week_of_month|   amt|          distance|is_fraud|            features|label|
  +-----------+-------------+------+------------------+--------+--------------------+-----+
  |          1|            4|125.02|48.876478206769406|       0|[1.0,4.0,125.02,4...|  0.0|
  |          3|            2|   3.1|111.47316521882695|       0|[3.0,2.0,3.1,111....|  0.0|
  |          5|            1|116.25|33.009955560792996|       0|[5.0,1.0,116.25,3...|  0.0|
  |          3|            2| 49.01|48.273248442946404|       0|[3.0,2.0,49.01,48...|  0.0|
  |          6|            2|158.76| 68.25966084141116|       0|[6.0,2.0,158.76,6...|  0.0|
  |          6|            1| 69.08|63.191419440317354|       0|[6.0,1.0,69.08,63...|  0.0|
  |          1|            2| 12.08|103.77304260763073|       0|[1.0,2.0,12.08,10...|  0.0|
  |          1|            5| 20.51| 38.94042126973156|       0|[1.0,5.0,20.51,38...|  0.0|
  |          7|            3| 16.63| 97.85432735787023|       0|[7.0,3.0,16.63,97...|  0.0|
  |          2|            4| 43.08| 41.70580386795137|       0|[2.0,4.0,43.08,41...|  0.0|
  +-----------+-------------+------+------------------+--------+--------------------+-----+
  only showing top 10 rows

  22/12/20 17:10:15 WARN BLAS: Failed to load implementation from: com.github.fommil.netlib.NativeSystemBLAS
  22/12/20 17:10:15 WARN BLAS: Failed to load implementation from: com.github.fommil.netlib.NativeRefBLAS
  22/12/20 17:10:15 INFO OWLQN: Step Size: 6.063
  22/12/20 17:10:15 INFO OWLQN: Val and Grad Norm: 0.0164598 (rel: 0.507) 0.0255365
  22/12/20 17:10:15 INFO OWLQN: Step Size: 1.000
  22/12/20 17:10:15 INFO OWLQN: Val and Grad Norm: 0.0167115 (rel: -0.0151) 0.0177362
  22/12/20 17:10:15 INFO OWLQN: Step Size: 1.000
  22/12/20 17:10:15 INFO OWLQN: Val and Grad Norm: 0.0147263 (rel: 0.119) 0.00890076
  22/12/20 17:10:15 INFO OWLQN: Step Size: 0.5000
  22/12/20 17:10:15 INFO OWLQN: Val and Grad Norm: 0.0144596 (rel: 0.0181) 0.00549600
  22/12/20 17:10:15 INFO OWLQN: Step Size: 1.000
  22/12/20 17:10:15 INFO OWLQN: Val and Grad Norm: 0.0143438 (rel: 0.00801) 0.00356065
  22/12/20 17:10:15 INFO OWLQN: Step Size: 1.000
  22/12/20 17:10:15 INFO OWLQN: Val and Grad Norm: 0.0141168 (rel: 0.0158) 0.00327394
  22/12/20 17:10:15 INFO OWLQN: Step Size: 1.000
  22/12/20 17:10:15 INFO OWLQN: Val and Grad Norm: 0.0127669 (rel: 0.0956) 0.00927931
  22/12/20 17:10:15 INFO OWLQN: Step Size: 1.000
  22/12/20 17:10:15 INFO OWLQN: Val and Grad Norm: 0.0126231 (rel: 0.0113) 0.00409302
  22/12/20 17:10:15 INFO OWLQN: Step Size: 1.000
  22/12/20 17:10:15 INFO OWLQN: Val and Grad Norm: 0.0125269 (rel: 0.00762) 0.000430521
  22/12/20 17:10:16 INFO OWLQN: Step Size: 1.000
  22/12/20 17:10:16 INFO OWLQN: Val and Grad Norm: 0.0125216 (rel: 0.000426) 0.000326559
  22/12/20 17:10:16 INFO OWLQN: Step Size: 1.000
  22/12/20 17:10:16 INFO OWLQN: Val and Grad Norm: 0.0125209 (rel: 5.61e-05) 0.000115333
  22/12/20 17:10:16 INFO OWLQN: Step Size: 1.000
  22/12/20 17:10:16 INFO OWLQN: Val and Grad Norm: 0.0125209 (rel: 4.63e-06) 2.80652e-05
  22/12/20 17:10:16 INFO OWLQN: Step Size: 1.000
  22/12/20 17:10:16 INFO OWLQN: Val and Grad Norm: 0.0125209 (rel: 1.53e-07) 7.65259e-06
  22/12/20 17:10:16 INFO OWLQN: Step Size: 1.000
  22/12/20 17:10:16 INFO OWLQN: Val and Grad Norm: 0.0125208 (rel: 1.54e-08) 1.66012e-06
  22/12/20 17:10:16 INFO OWLQN: Step Size: 1.000
  22/12/20 17:10:16 INFO OWLQN: Val and Grad Norm: 0.0125208 (rel: 7.79e-10) 4.35944e-07
  22/12/20 17:10:16 INFO OWLQN: Step Size: 1.000
  22/12/20 17:10:16 INFO OWLQN: Val and Grad Norm: 0.0125208 (rel: 5.47e-11) 1.25418e-07
  22/12/20 17:10:16 INFO OWLQN: Step Size: 1.000
  22/12/20 17:10:16 INFO OWLQN: Val and Grad Norm: 0.0125208 (rel: 4.51e-12) 3.61748e-08
  22/12/20 17:10:16 INFO OWLQN: Step Size: 1.000
  22/12/20 17:10:16 INFO OWLQN: Val and Grad Norm: 0.0125208 (rel: 2.56e-13) 8.98009e-09
  22/12/20 17:10:16 INFO OWLQN: Converged because gradient converged
  root
   |-- day_of_week: integer (nullable = true)
   |-- week_of_month: integer (nullable = true)
   |-- amt: double (nullable = true)
   |-- distance: double (nullable = true)
   |-- is_fraud: integer (nullable = true)
   |-- features: vector (nullable = true)
   |-- label: double (nullable = false)
   |-- rawPrediction: vector (nullable = true)
   |-- probability: vector (nullable = true)
   |-- prediction: double (nullable = false)

  +-----------+-------------+------+-----------------+--------+--------------------+-----+--------------------+--------------------+----------+
  |day_of_week|week_of_month|   amt|         distance|is_fraud|            features|label|       rawPrediction|         probability|prediction|
  +-----------+-------------+------+-----------------+--------+--------------------+-----+--------------------+--------------------+----------+
  |          1|            1|790.85|56.21262482251961|       1|[1.0,1.0,790.85,5...|  1.0|[-1.1057480238767...|[0.24866443786988...|       1.0|
  |          1|            1|853.59|45.38135817575257|       1|[1.0,1.0,853.59,4...|  1.0|[-1.1057480238767...|[0.24866443786988...|       1.0|
  +-----------+-------------+------+-----------------+--------+--------------------+-----+--------------------+--------------------+----------+
  only showing top 2 rows

  +-----------+-------------+----+------------------+--------+--------------------+-----+--------------------+--------------------+----------+
  |day_of_week|week_of_month| amt|          distance|is_fraud|            features|label|       rawPrediction|         probability|prediction|
  +-----------+-------------+----+------------------+--------+--------------------+-----+--------------------+--------------------+----------+
  |          1|            1|1.04| 74.59775874501095|       0|[1.0,1.0,1.04,74....|  0.0|[6.61361579252029...|[0.99865982739546...|       0.0|
  |          1|            1|1.05| 52.00672407293104|       0|[1.0,1.0,1.05,52....|  0.0|[6.61361579252029...|[0.99865982739546...|       0.0|
  |          1|            1|1.15|108.52646896893121|       0|[1.0,1.0,1.15,108...|  0.0|[6.61361579252029...|[0.99865982739546...|       0.0|
  +-----------+-------------+----+------------------+--------+--------------------+-----+--------------------+--------------------+----------+
  only showing top 3 rows

  Prediction Accuracy: 1.0

  Process finished with exit code 0

}

 */
