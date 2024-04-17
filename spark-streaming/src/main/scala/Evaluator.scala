import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.StringIndexerModel
import io.github.cdimascio.dotenv.Dotenv

object Evaluator extends App{

  val dotenv = Dotenv.configure().load();
  val admin_email = dotenv.get("ADMIN_EMAIL")
  val spark = SparkSession.builder()
  .appName("StringIndexerTransformer")
  .config("spark.master", "local[2]")
  .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  val pipelineRfModelPath = "../model/resources/models/pipelineModelRandomForest"
  val loadedModel = PipelineModel.load(pipelineRfModelPath)
  /*
  Loading the StringIndexer Model that was used while training the Random Forest Model
  This will be used to map the numerical values back to string labels in the attack predictions
   */
  val stringIndexerModel = loadedModel.stages.last.asInstanceOf[StringIndexerModel]
  val labels = stringIndexerModel.labelsArray


  /*
  Creating an immutable dictionary mapping each index in prediction.csv to the corresponding string value
   */
  val dictionary: Map[Int, String] = labels(0).zipWithIndex.map { case (value, index) => (index, value) }.toMap

  /*
  Reading all the csv files to process the Log Data Batch
   */
  val resultSchema = new StructType()
    .add(name = "prediction", DoubleType, nullable = false)
  val outputResultPath = "src/resources/result/csv/"
  val df = spark.read
    .format("csv")
    .option("header", true)
    .option("sep", ",")
    .schema(resultSchema)
    .load(outputResultPath + "/*.csv")


  /*
  Processing the predictions (numerical values) and mapping them to integer values
  These integer values will then be mapped back to the string values, using the dictionary created above
   */
  val intdf = df.select("prediction").withColumn("predictionInteger", col("prediction").cast("integer"))
  val mapPrediction: UserDefinedFunction = udf((prediction: Int) => dictionary.getOrElse(prediction, "new_attack"))
  val dfWithMappedColumn = intdf.withColumn("predictionString", mapPrediction(col("predictionInteger")))
  println(dfWithMappedColumn.count())


  /*
    4 categories of attacks overall
     DOS: denial-of-service, e.g. syn flood;
     R2L: unauthorized access from a remote machine, e.g. guessing password;
     U2R:  unauthorized access to local superuser (root) privileges, e.g., various ``buffer overflow'' attacks;
     Probing: surveillance and other probing, e.g., port scanning.
     */
  /*
  Sub-categories or labels (used while training the model)
  Each sub-category comes under the umbrella of an attack type
  This Map holds the mapping of each of the Sub-category mapped to the attach type
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
    "warezmaster" -> "r2l",
    "new_attack" -> "Unknown"
  )



  /*
  Mapping the predictions into 4 kinds of the attacks using the category map
  Using Unknown if a new kind of sub-category is encountered
   */
  val mapCategory = udf((string: String) => categoryMap.getOrElse(string, "Unknown"))
  val dfWithCategory = dfWithMappedColumn.withColumn("category", mapCategory(col("predictionString")))
  val attackTypes = Seq("probe", "dos", "r2l", "u2r", "Unknown")


  /*
  Creating Pie Chart for the distribution of each of the attach type
  This Pie Chart will have a distribution of the total records categorized as NORMAL, PROBE, DOS, R2L, U2L
   */
  val plot = new CreatePlot()
  val plotsBasePath = "src/resources/result/plots/"
  plot.createPieChart(dfWithCategory, plotsBasePath)
  val groupedDf = dfWithCategory.groupBy("category", "predictionString").count()


  /*
  Converting to a Seq of (category, predictionString, count) tuples
   */
  val dataSeq = groupedDf.collect().map { row =>
    val category = row.getAs[String]("category")
    val predictionString = row.getAs[String]("predictionString")
    val count = row.getAs[Long]("count")
    (category, predictionString, count)
  }.toSeq


  val groupedByValue = categoryMap.groupBy(_._2).mapValues(_.keys.toSeq)
  /*
  Plotting  Bar Chart for attack type signatures for each attack
  This method show the sub-categories of each of the attack type
  We display which sub-categories belong to each attack type
  What is the count distribution of each of the sub category
   */
  attackTypes.foreach(attack => {
    val labels = dataSeq.filter(_._1 == attack).map(_._2)
    val counts = dataSeq.filter(_._1 == attack).map(_._3)

    val allLabels = groupedByValue.getOrElse(attack, Seq.empty)
    val allCounts = allLabels.map(label => {
      val index = labels.indexOf(label)
      if (index != -1) counts(index).toDouble else 0.0
    })
    plot.createBarChart(allCounts,allLabels, attack, plotsBasePath)


  })

  /*
   Finding attack percentage for this batch of log files,
   Attack percentage can be used to implement trigger alarms

   */
  val normalCount = dfWithCategory.filter(col("category") === "normal").count()
  val totalCount = dfWithCategory.count()
  val attackPercent = ((totalCount.toDouble - normalCount.toDouble) / totalCount.toDouble) * 100

  /*
  Mailing the Network Report using AWS setup
  We have a detailed setup, to send a detailed report with all our findings to an email
  We have used Amazon Simple Notification Service for this implementation
   */
  val sendEmailReport = new SendEmailReport
  sendEmailReport.sendEmail(admin_email, attackPercent, plotsBasePath)


  /*
    Persisting the predictions for future references
     */
  Utils.transferCSV("src/resources/result/csv", "src/resources/result/storeCSV")



  spark.stop()

}
