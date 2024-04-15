import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object TestPipelineModel extends App {

  val spark = SparkSession.builder()
    .appName("Random Forest Classifier")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR") // We want to ignore all of the INFO and WARN messages.

  val eventDataSchema = new StructType()
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
   // .add("attack", StringType, nullable = false)
   // .add("last_flag", StringType, nullable = false)

  val df = spark.read
    .format("csv")
    //.option("header", true)
    .option("sep", ",")
    .schema(eventDataSchema)
    .load("/Users/unnatiaggarwal/Documents/CSYE7200-PROJECT/final-csye7200-project/csye7200-project/spark-streaming/src/resources/data/Train.csv")

  df.printSchema()


  val stringColumns = Array(
    "protocol_type",
    "service",
    "flag"
  )

  val numericalColumns = Array(
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

  // Step 2: Create StringIndexers for string columns
  val stringIndexers = stringColumns.map { col =>
    new StringIndexer()
      .setInputCol(col)
      .setOutputCol(s"${col}_index")
  }

  // Step 3: Assemble string columns into a single feature vector
  val stringAssembler = new VectorAssembler()
    .setInputCols(stringColumns.map(_ + "_index"))
    .setOutputCol("string_features")

  // Step 4: Assemble numerical columns into a single feature vector
  val numericalAssembler = new VectorAssembler()
    .setInputCols(numericalColumns)
    .setOutputCol("numerical_features")

  // Step 5: Combine both string and numerical feature vectors into a final feature vector
  val assembler = new VectorAssembler()
    .setInputCols(Array("string_features", "numerical_features"))
    .setOutputCol("features")


  // Step 6: Define a pipeline
  val pipeline = new Pipeline().setStages(stringIndexers ++ Array(stringAssembler, numericalAssembler, assembler ))
  val pipelineModel = pipeline.fit(df)



  val pipelineModelPath = "model/resources/models/pipelineModeltest"
  pipelineModel.write.overwrite().save(pipelineModelPath)

}
