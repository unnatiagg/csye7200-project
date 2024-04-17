import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object RandomForestClassifier extends App {
  val spark = SparkSession.builder()
    .appName("RandomForestClassifier")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def getEventDataSchema: StructType = {
    new StructType()
      .add("duration", IntegerType, nullable = false)
      .add("protocol_type", StringType, nullable = false)
      .add("service", StringType, nullable = false)
      .add("flag", StringType, nullable = false)
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
      .add("attack", StringType, nullable = false)
      .add("last_flag", StringType, nullable = false)
  }

  def readData(filePath: String, schema: StructType, spark: SparkSession): DataFrame = {
    spark.read
      .format("csv")
      .option("sep", ",")
      .schema(schema)
      .load(filePath)
  }

  val eventDataSchema : StructType  = getEventDataSchema

  val df = readData("src/resources/data/Train.csv", eventDataSchema, spark)

  df.printSchema()

  def getStringColumns: Array[String] = {
    Array(
      "protocol_type",
      "service",
      "flag"
    )
  }

  def getNumericalColumns: Array[String] = {
    Array(
      "duration",
      "src_bytes",
      "dst_bytes",
      "land",
      "wrong_fragment",
      "urgent",
      "hot",
      "num_failed_logins",
      "logged_in",
      "num_compromised",
      "root_shell",
      "su_attempted",
      "num_root",
      "num_file_creations",
      "num_shells",
      "num_access_files",
      "num_outbound_cmds",
      "is_host_login",
      "is_guest_login",
      "count",
      "srv_count",
      "serror_rate",
      "srv_serror_rate",
      "rerror_rate",
      "srv_rerror_rate",
      "same_srv_rate",
      "diff_srv_rate",
      "srv_diff_host_rate",
      "dst_host_count",
      "dst_host_srv_count",
      "dst_host_same_srv_rate",
      "dst_host_diff_srv_rate",
      "dst_host_same_src_port_rate",
      "dst_host_srv_diff_host_rate",
      "dst_host_serror_rate",
      "dst_host_srv_serror_rate",
      "dst_host_rerror_rate",
      "dst_host_srv_rerror_rate"
    )
  }

  val stringColumns = getStringColumns

  val numericalColumns = getNumericalColumns

  def createProcessingPipeline(df: DataFrame, stringColumns: Array[String], numericalColumns: Array[String], pipelinePath: String): DataFrame = {
    /*
    Adding the various transformation to do preprocessing on the data
     */
    val stringIndexers = stringColumns.map { col =>
      new StringIndexer()
        .setInputCol(col)
        .setOutputCol(s"${col}_index")
    }

    val stringAssembler = new VectorAssembler()
      .setInputCols(stringColumns.map(_ + "_index"))
      .setOutputCol("string_features")

    val numericalAssembler = new VectorAssembler()
      .setInputCols(numericalColumns)
      .setOutputCol("numerical_features")

    val assembler = new VectorAssembler()
      .setInputCols(Array("string_features", "numerical_features"))
      .setOutputCol("features")

    val labelIndexer = new StringIndexer()
      .setInputCol("attack")
      .setOutputCol("label")
      .setHandleInvalid("keep")


    val pipeline = new Pipeline().setStages(stringIndexers ++ Array(stringAssembler, numericalAssembler, assembler, labelIndexer))
    val pipelineModel = pipeline.fit(df)
    val data = pipelineModel.transform(df).select("features", "label")
    pipelineModel.write.overwrite().save(pipelinePath)
    data
  }

  val pipelineModelPath = "../model/resources/models/pipelineModelRandomForest"
  val Array(train, test) = createProcessingPipeline(df, stringColumns,numericalColumns, pipelineModelPath ).randomSplit(Array(0.7, 0.3))

  def createRandomForestClassifier(labelCol: String, featuresCol: String, maxBins: Int, numTrees: Int): RandomForestClassifier = {
    new RandomForestClassifier()
      .setLabelCol(labelCol)
      .setFeaturesCol(featuresCol)
      .setMaxBins(maxBins)
      .setNumTrees(numTrees)
  }

  val rf = createRandomForestClassifier("label", "features", 100, 10)

    /*new RandomForestClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setMaxBins(100)
    .setNumTrees(10) // Setting the number of trees in the forest

     */

  val lrmodelPath = "../model/resources/models/random_forest_classification_model"

  def fitAndSaveModel(train: DataFrame, modelPath: String, rf: RandomForestClassifier): RandomForestClassificationModel =
    {
      val model: RandomForestClassificationModel = rf.fit(train)
      model.write.overwrite().save(modelPath)
      model
    }

  val modelLr = fitAndSaveModel(train, lrmodelPath, rf)
  def predictAndEvaluate(test: DataFrame, model:RandomForestClassificationModel): Double ={
    val predictionsLr = model.transform(test)
    val evaluatorLr = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val elr = evaluatorLr.evaluate(predictionsLr)
    println("--- Random Forest Classifier --- ")
    println(s"Accuracy Rate = ${"%.4f".format(elr)}")
    println(s"  Error  Rate = ${"%.4f".format(1.0 - elr)}")
    elr
  }

  val evaluation =  predictAndEvaluate(test, modelLr)






}
