import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.UserDefinedFunction

import java.io.File
import java.nio.file.{Files, Paths}

object Evaluator extends App{
// Initialize Spark session
val spark = SparkSession.builder()
  .appName("StringIndexerTransformer")
  .config("spark.master", "local[2]")
  .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


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
    //.add(name = "label", DoubleType, nullable = false)
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

  val groupedByValue = categoryMap.groupBy(_._2).mapValues(_.keys.toSeq)

  // Define a User Defined Function (UDF) to map strings to categories
  val mapCategory = udf((string: String) => categoryMap.getOrElse(string, "Unknown"))

  // Apply the UDF to map strings to categories and create a new column
  val dfWithCategory = dfWithMappedColumn.withColumn("category", mapCategory(col("predictionString")))

  val attackTypes = Seq("probe", "dos", "r2l", "u2r")


//  Create Pie Chart
  val plot = new CreatePlot()

  plot.createPieChart(dfWithCategory, "pieChart.png")



  // Assuming dfWithCategory is the DataFrame with the "category" column
  val groupedDf = dfWithCategory.groupBy("category", "predictionString").count()

  // Convert to a Seq of (category, predictionString, count) tuples
  val dataSeq = groupedDf.collect().map { row =>
    val category = row.getAs[String]("category")
    val predictionString = row.getAs[String]("predictionString")
    val count = row.getAs[Long]("count")
    (category, predictionString, count)
  }.toSeq



// Plot Bar Chart for attack type signatures for each attack
  attackTypes.foreach(attack => {
    val labels = dataSeq.filter(_._1 == attack).map(_._2)
    val counts = dataSeq.filter(_._1 == attack).map(_._3)

    val allLabels = groupedByValue.getOrElse(attack, Seq.empty)
    val allCounts = allLabels.map(label => {
      val index = labels.indexOf(label)
      if (index != -1) counts(index).toDouble else 0.0
    })


    plot.createBarChart(allCounts,allLabels, attack)


  })

//  Find attack percentage
  val normalCount = dfWithCategory.filter(col("category") === "normal").count()
  val totalCount = dfWithCategory.count()
  val attackPercent = ((totalCount.toDouble - normalCount.toDouble) / totalCount.toDouble) * 100

  val sendEmailReport = new SendEmailReport
  sendEmailReport.sendEmail("sachdeva.sh@northeastern.edu", attackPercent, "src/resources/result/plots")

  val csvPath = "src/resources/result/csv"
  val storeCSVPath = "src/resources/result/storeCSV"

  val csvDirectory = new File(csvPath)
  val storeCSVDirectory = new File(storeCSVPath)

  if (!storeCSVDirectory.exists()) {
    storeCSVDirectory.mkdir()
  }

  if (csvDirectory.exists() && csvDirectory.isDirectory) {
    val csvFiles = csvDirectory.listFiles().filter(_.getName.endsWith(".csv"))
    csvFiles.foreach { file =>
      val sourcePath = file.toPath
      val destPath = Paths.get(storeCSVPath, file.getName)
      Files.move(sourcePath, destPath)
      println(s"Moved ${file.getName} to $storeCSVPath")
    }
  } else {
    println("CSV directory does not exist or is not a directory")
  }



  spark.stop()

}
