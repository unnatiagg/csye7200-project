import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, explode, from_json, lit}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}



object SparkStreamingNetworkData {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkStreaming")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val lrmodelPath = "src/resources/models/logistic_regresssion"
    val lrModel = LogisticRegressionModel.load(lrmodelPath)

    val pipelineModelPath = "src/resources/models/pipelineModel"
    val pipelineModel = PipelineModel.load(pipelineModelPath)

    val outputResultPath = "src/resources/result/lr_output.csv"

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "data-stream")
      .option("startingOffsets", "latest")
      .load()

    import org.apache.spark.sql.types._

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

    val songs = df.selectExpr("cast(value as string)")
      .select(from_json(col("value"), eventDataSchema).as("data"))
      .select("data.*")

    val q = songs
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false) // To avoid truncating the output
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val count = batchDF.count()
        println(s"Batch ID: $batchId, Count: $count")

        batchDF.printSchema()
        if (count>0)
          {
            // Transform the test data using the pipeline model
            val testData = pipelineModel.transform(batchDF).select("features", "label")

            // Make predictions using the logistic regression model
            val predictions = lrModel.transform(testData)

            // Display the predictions
            predictions.show()
//            predictions.printSchema()

            // Save the result
            // Append the batchDF and predictions
//            val resultDF = batchDF.join(predictions, Seq("prediction"), "inner")
//
//            // Append all of them and save them in a file
//            resultDF
//              .write
//              .mode("append")
//              .format("csv")
//              .option("header", "true")
//              .save(outputResultPath)

            val evaluatorLr = new MulticlassClassificationEvaluator()
              .setLabelCol("label")
              .setPredictionCol("prediction")
              .setMetricName("accuracy")

            val elr = evaluatorLr.evaluate(predictions)

            // Printing the results
            println("--- Logistic Regression --- ")
            println(s"Accuracy Rate = ${"%.4f".format(elr)}")
            println(s"  Error  Rate = ${"%.4f".format(1.0 - elr)}")

          }
;
      }
      .start()

    q.awaitTermination()

  }
}