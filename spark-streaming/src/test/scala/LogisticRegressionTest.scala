import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.ml.classification.{LogisticRegressionModel, LogisticRegression}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import java.io.File

class LogisticRegressionTestSpec extends AnyFlatSpec with Matchers {

  // Testing getEventDataSchema method
  "getEventDataSchema" should "return a StructType with specific schema" in {
    val schema = LogisticRegression.getEventDataSchema

    val expectedSchema = new StructType()
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
    schema shouldEqual expectedSchema
  }

  // Testing readData method
  "readData" should "read data from CSV file and return DataFrame" in {
    val spark = SparkSession.builder()
      .appName("LogisticRegressionTest")
      .master("local[*]")
      .getOrCreate()
    val schema = LogisticRegression.getEventDataSchema
    val testDataFrame = LogisticRegression.readData("src/test/scala/Train.csv", schema, spark)
    testDataFrame should not be null

  }

  // Testing getStringColumns method
  "getStringColumns" should "return an array of string columns" in {
    val columns =LogisticRegression.getStringColumns
    val expectedColumns = Array("protocol_type", "service", "flag")
    columns shouldEqual expectedColumns
  }

  // Testing getNumericalColumns method
  "getNumericalColumns" should "return an array of numerical columns" in {
    val columns = LogisticRegression.getNumericalColumns
    // val expectedColumns = Array("duration", "src_bytes", "dst_bytes", ...)
    columns.length shouldEqual 38
  }

  // Testing createProcessingPipeline method
  "createProcessingPipeline" should "create and save pipeline model" in {
    val spark = SparkSession.builder()
      .appName("LogisticRegressionTest")
      .master("local[*]")
      .getOrCreate()
    val schema = LogisticRegression.getEventDataSchema
    val df = LogisticRegression.readData("src/test/scala/Train.csv", schema, spark)
    val stringColumns = LogisticRegression.getStringColumns
    val numericalColumns = LogisticRegression.getNumericalColumns
    val pipelineModelPath = "src/test/scala/pipeline_lr_test"
    val processedData = LogisticRegression.createProcessingPipeline(df, stringColumns, numericalColumns, pipelineModelPath)
    // Check if the pipeline model is saved at the specified path
    val pipelineModelFile = new File(pipelineModelPath)
    pipelineModelFile.exists() shouldEqual true
  }

  // Testing createLogisticRegression method
  "createLogisticRegression" should "create a LogisticRegression instance" in {
    val lr = LogisticRegression.createLogisticRegression()
    lr shouldBe a[LogisticRegression]
  }

  // Test fitAndSaveModel method
  "fitAndSaveModel" should "fit the model and save it" in {
    val spark = SparkSession.builder()
      .appName("LogisticRegressionTest")
      .master("local[*]")
      .getOrCreate()
    val schema = LogisticRegression.getEventDataSchema
    val df = LogisticRegression.readData("src/test/scala/train.csv", schema, spark)
    val pipelinePath = "src/test/scala/pipeline_lr_test"
    val pipeline = PipelineModel.load(pipelinePath)
    val processedDf = pipeline.transform(df).select("features", "label")
    val lr = LogisticRegression.createLogisticRegression()
    val modelPath = "src/test/scala/model_lr_test"
    val model = LogisticRegression.fitAndSaveModel(processedDf, modelPath, lr)
    // Check if the model is saved at the specified path
    val modelFile = new File(modelPath)
    modelFile.exists() shouldEqual true
    model shouldBe a[LogisticRegressionModel]
  }

  // Test predictAndEvaluate method
  "predictAndEvaluate" should "predict and evaluate the model" in {
    val spark = SparkSession.builder()
      .appName("LogisticRegressionTest")
      .master("local[*]")
      .getOrCreate()
    val schema = LogisticRegression.getEventDataSchema
    val df = LogisticRegression.readData("src/test/scala/Test.csv", schema, spark)
    val modelPath = "src/test/scala/model_lr_test"
    val model = LogisticRegressionModel.load(modelPath)
    val pipelinePath = "src/test/scala/pipeline_lr_test"
    val pipeline = PipelineModel.load(pipelinePath)


    val testDf = pipeline.transform(df).select("features", "label")

    val accuracy = LogisticRegression.predictAndEvaluate(testDf, model)
    // Ensure that accuracy is within acceptable range
    println(accuracy)
    accuracy should (be >= 0.0 and be <= 1.0)
  }
}
