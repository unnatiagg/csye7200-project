import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.{IndexToString, StringIndexerModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.UserDefinedFunction



object Preprocessing extends App{
// Initialize Spark session
val spark = SparkSession.builder()
  .appName("StringIndexerTransformer")
  .config("spark.master", "local[2]")
  .getOrCreate()

// Read the CSV file containing predictions and labels
//val data = spark.read.option("header", "true").option("inferSchema", "true").csv("src/resources/result/csv/part-00000-0ef4a43e-98d6-4004-b987-7640cc96e5f5-c000.csv")

// Assuming you have columns named "predictions" and "labels"
//val predictionsAndLabels = data.select("prediction", "label")
//
//  predictionsAndLabels.show()

  val pipelineRfModelPath = "/Users/unnatiaggarwal/Documents/CSYE7200-PROJECT/final-csye7200-project/csye7200-project/model/resources/models/pipelineModel2"

  import org.apache.spark.ml.PipelineModel
  import org.apache.spark.ml.feature.StringIndexerModel

  // Load the saved pipeline model
  val loadedModel = PipelineModel.load(pipelineRfModelPath)

  // Extract the StringIndexerModel from the pipeline model
  val stringIndexerModel = loadedModel.stages.last.asInstanceOf[StringIndexerModel]

  // Get the labels used for indexing
  val labels = stringIndexerModel.labelsArray

  // Print or use the mapping as needed
  println("Mapping between labels and indices:")
  labels.foreach { labelArray =>
    labelArray.foreach( u => println(u))
  }

  // Create an immutable dictionary mapping each index to the corresponding value
  val dictionary: Map[Int, String] = labels(0).zipWithIndex.map { case (value, index) => (index, value) }.toMap

  // Display the dictionary
  println(dictionary)

  val resultSchema = new StructType()
    .add(name = "label", DoubleType, nullable = false)
    .add(name = "prediction", DoubleType, nullable = false)

  val outputResultPath = "src/resources/result/csv/"

  val df = spark.read
    .format("csv")
    .option("header", true)
    .option("sep", ",")
    .schema(resultSchema)
    .load(outputResultPath + "/*.csv")

 // df.select("prediction").show(20)
   val intdf = df.select("prediction").withColumn("predictionInteger", col("prediction").cast("integer"))

  // Define a User Defined Function (UDF) to map integers to strings
  val mapPrediction: UserDefinedFunction = udf((prediction: Int) => dictionary.getOrElse(prediction, "new_attack"))

  // Apply the UDF to map integers to strings and create a new column
  val dfWithMappedColumn = intdf.withColumn("predictionString", mapPrediction(col("predictionInteger")))

  println(dfWithMappedColumn.count())
  dfWithMappedColumn.groupBy("predictionString").count().show(30)


  /*
  back, dos
  buffer_overflow, u2r
  ftp_write, r2l
  guess_passwd, r2l
  imap, r2l
  ipsweep, probe
  land, dos
  loadmodule, u2r
  multihop, r2l
  neptune, dos
  nmap, probe
  perl, u2r
  phf, r2l
  pod, dos
  portsweep, probe
  rootkit, u2r
  satan, probe
  smurf, dos
  spy, r2l
  teardrop, dos
  warezclient, r2l
  warezmaster, r2l


  DOS: denial-of-service, e.g. syn flood;
  R2L: unauthorized access from a remote machine, e.g. guessing password;
  U2R:  unauthorized access to local superuser (root) privileges, e.g., various ``buffer overflow'' attacks;
  Probing: surveillance and other probing, e.g., port scanning.
   */
  val categoryMap = Map(
    "normal" -> "normal",
    "buffer_overflow" -> "u2r",
    "back" -> "dos",
    "ftp_write" -> "r2l",
    "guess_passwd" -> "r2l",
    "imap" -> "r2l",
    "ipsweep" -> "probe",
    "land" -> "dos",
    "loadmodule" -> "u2r",
    "multihop" -> "r2l",
    "neptune" -> "dos",
    "nmap" -> "probe",
    "perl" -> "u2r",
    "phf" -> "r2l",
    "pod" -> "dos",
    "portsweep" -> "probe",
    "rootkit" -> "u2r",
    "satan" -> "probe",
    "smurf" -> "dos",
    "spy" -> "r2l",
    "teardrop" -> "dos",
    "warezclient" -> "r2l",
    "warezmaster" -> "r2l"
  )


  // Define a User Defined Function (UDF) to map strings to categories
  val mapCategory = udf((string: String) => categoryMap.getOrElse(string, "Unknown"))

  // Apply the UDF to map strings to categories and create a new column
  val dfWithCategory = dfWithMappedColumn.withColumn("category", mapCategory(col("predictionString")))

  dfWithCategory.show()

  dfWithCategory.groupBy("category").count().show(30)
  spark.stop()


}
