import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object LogisticRegression extends App {

  print("Hello")

  val spark = SparkSession.builder()
    .appName("LogisticRegression")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR") // We want to ignore all of the INFO and WARN messages.

  val eventDataSchema = new StructType()
    .add("duration", IntegerType, nullable = false)
    .add("src_bytes", IntegerType, nullable = false)
    .add("dst_bytes", IntegerType, nullable = false)
    .add("land", IntegerType, nullable = false)
    .add("wrong_fragment", IntegerType, nullable = false)
    .add("urgent", IntegerType, nullable = false)
    .add("hot", IntegerType, nullable = false)
    .add("num_failed_logins", IntegerType, nullable = false)
    .add("logged_in", IntegerType, nullable = false)
    .add("num_compromised", IntegerType, nullable = false)
    .add("root_shell", IntegerType, nullable = false)
    .add("su_attempted", IntegerType, nullable = false)
    .add("num_root", IntegerType, nullable = false)
    .add("num_file_creations", IntegerType, nullable = false)
    .add("num_shells", IntegerType, nullable = false)
    .add("num_access_files", IntegerType, nullable = false)
    .add("num_outbound_cmds", IntegerType, nullable = false)
    .add("is_host_login", IntegerType, nullable = false)
    .add("is_guest_login", IntegerType, nullable = false)
    .add("count", IntegerType, nullable = false)
    .add("srv_count", IntegerType, nullable = false)
    .add("serror_rate", DoubleType, nullable = false)
    .add("srv_serror_rate", DoubleType, nullable = false)
    .add("rerror_rate", DoubleType, nullable = false)
    .add("srv_rerror_rate", DoubleType, nullable = false)
    .add("same_srv_rate", DoubleType, nullable = false)
    .add("diff_srv_rate", DoubleType, nullable = false)
    .add("srv_diff_host_rate", DoubleType, nullable = false)
    .add("dst_host_count", IntegerType, nullable = false)
    .add("dst_host_srv_count", IntegerType, nullable = false)
    .add("dst_host_same_srv_rate", DoubleType, nullable = false)
    .add("dst_host_diff_srv_rate", DoubleType, nullable = false)
    .add("dst_host_same_src_port_rate", DoubleType, nullable = false)
    .add("dst_host_srv_diff_host_rate", DoubleType, nullable = false)
    .add("dst_host_serror_rate", DoubleType, nullable = false)
    .add("dst_host_srv_serror_rate", DoubleType, nullable = false)
    .add("dst_host_rerror_rate", DoubleType, nullable = false)
    .add("dst_host_srv_rerror_rate", DoubleType, nullable = false)
    .add("status", StringType, nullable = false)

  val df = spark.read
  .format("csv")
  .option("header", true)
  .option("sep", ",")
  .schema(eventDataSchema)
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
  val lrmodelPath = "src/resources/models/logistic_regresssion"
  modelLr.write.overwrite().save(lrmodelPath)

  val pipelineModelPath = "src/resources/models/pipelineModel"
  pipelineModel.write.overwrite().save(pipelineModelPath)

}
