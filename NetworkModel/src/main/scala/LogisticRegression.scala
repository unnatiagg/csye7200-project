import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object LogisticRegression extends App {

  print("Hello")

  val spark = SparkSession.builder()
    .appName("LogisticRegression")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR") // We want to ignore all of the INFO and WARN messages.

  val df = spark.read
  .format("csv")
  .option("header", true)
  .option("sep", ",")
  .option("inferSchema", "True")
  .load("src/resources/data/TrainDf.csv")

  df.printSchema()

//  Data Preparation

  val featureCols = df.columns.slice(0, df.columns.length - 1)
  val labelIndexer = new StringIndexer()
    .setInputCol("status")
    .setOutputCol("label")

  val assembler = new VectorAssembler()
    .setInputCols(featureCols)
    .setOutputCol("features")

  val stages = Array(assembler, labelIndexer)
  val pipeline = new Pipeline().setStages(stages)
  val pipelineModel = pipeline.fit(df)
  val data = pipelineModel.transform(df).select("features", "label")
//
  val Array(train, test) = data.randomSplit(Array(0.7, 0.3))

  val lr = new LogisticRegression().setRegParam(0.01)
  val modelLr = lr.fit(train)

  // Making predictions
  val predictionsLr = modelLr.transform(test)

  // Displaying the first three rows of the prediction DataFrame
  predictionsLr.show(3)

  // Creating a MulticlassClassificationEvaluator
  val evaluatorLr = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")

  // Evaluating the model
  val elr = evaluatorLr.evaluate(predictionsLr)

  // Printing the results
  println("--- Logistic Regression --- ")
  println(s"Accuracy Rate = ${"%.4f".format(elr)}")
  println(s"  Error  Rate = ${"%.4f".format(1.0 - elr)}")

  // Saving the model
  val modelPath = "src/resources/models/logistic_regresssion"
  pipelineModel.write.overwrite().save(modelPath)

  // Load the model later using:
  // val loadedModel = PipelineModel.load(modelPath)


}
