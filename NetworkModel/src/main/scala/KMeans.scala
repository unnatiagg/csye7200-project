import org.apache.spark.sql.{SparkSession, DataFrame, functions}
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.UserDefinedFunction



object KMeans extends App {

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

  val labelIndexer = new StringIndexer()
    .setInputCol("status")
    .setOutputCol("label")

  val labelIndexerModel = labelIndexer.fit(df)
  val newDf = labelIndexerModel.transform(df)

  val featureCols = df.columns.dropRight(1) // Drop the last column ('status')
  val assembler = new VectorAssembler()
    .setInputCols(featureCols)
    .setOutputCol("vec_features")

  val assemblerDf = assembler.transform(newDf)

  val normal = assemblerDf.filter(functions.col("status") === "normal")

  val pca = new PCA()
    .setK(9)
    .setInputCol("vec_features")
    .setOutputCol("features")

  val pcaModel = pca.fit(normal)
  val normalReductionDf = pcaModel.transform(normal)

  val kNum = 2
  val kmeans = new KMeans()
    .setFeaturesCol("features")
    .setK(kNum)
    .setMaxIter(100)

//  Training Of Data

  val model = kmeans.fit(normalReductionDf)
  // Make predictions
  val predictions = model.transform(normalReductionDf)

  // Evaluate clustering by computing Within Set Sum of Squared Errors
  val evaluator = new ClusteringEvaluator()
  val wssse = evaluator.evaluate(predictions)

  println(s"With K= $kNum")
  println(s"Within Set Sum of Squared Errors = $wssse")
  println("--" * 30)

//  Prediction of Training Dataset

  val training_dataset_pca = new PCA()
    .setK(9)
    .setInputCol("vec_features")
    .setOutputCol("features")

  val tdpcaModel = training_dataset_pca.fit(assemblerDf)
  val testReductionDf = pcaModel.transform(assemblerDf)

  val training_dataset_predictions = model.transform(testReductionDf)
  val selected = training_dataset_predictions.select(col("features"), col("label"), col("prediction"))
  selected.show(5)

//  Calculation of Silhouette Score
  val silhouette_evaluator = new ClusteringEvaluator()
    .setFeaturesCol("features")
    .setPredictionCol("prediction")
    .setMetricName("silhouette")

  val silhouette = silhouette_evaluator.evaluate(predictions)

  println(s"Silhouette with squared euclidean distance = $silhouette")

//  Analysing of Training Model
  val grouped = predictions
    .select("prediction", "label")
    .groupBy("prediction", "label")
    .agg(count("*").as("count"))
    .orderBy("prediction", "label")

  val result = grouped.toDF().orderBy(col("prediction"), col("label")).limit(10)
  result.show()

  val grouped_labels = predictions
    .select("prediction", "label")
    .groupBy("prediction", "label")
    .agg(count("*").as("count"))
    .orderBy("prediction", "label")

  val result_labels = grouped_labels
    .withColumn("status", when(col("label") === 1, "Anomaly").otherwise("Normal"))
    .orderBy(col("prediction"), col("label"))
    .limit(10)

  result_labels.show()

//  Calculation of centroids for every cluster
  val trainClusters = model.clusterCenters.map(_.toArray)

  val traindClusters = trainClusters.indices.map { i =>
    i -> trainClusters(i).map(_.toFloat).toList
  }.toMap

  trainClusters


  import spark.implicits._

  val trainDfCenters = spark.createDataFrame(traindClusters.toSeq)
    .toDF("prediction", "center")

  trainDfCenters.show(5)

  val trainPredDf = predictions.withColumn("prediction", col("prediction").cast(IntegerType))
  val selectedColumns = trainPredDf.select("features", "label", "prediction")
  selectedColumns.show(5)

//  Joining of centroids and feature dataframes
  val trainPredDf_centroid = trainPredDf.join(trainDfCenters, Seq("prediction"), "left")
  val selectedColumns_centroid = trainPredDf_centroid.select("features", "label", "prediction", "center")
  selectedColumns_centroid.show(5)
//  trainPredDf_centroid.show(5)

  val get_dist: Vector => Vector => Float = features => center =>
    Vectors.sqdist(features, center).toFloat

  val get_dist_udf = udf((features: Vector, center: Vector) => {
    val featuresArray = features.toArray.map(_.toFloat)
    val centerArray = center.toArray.map(_.toFloat)
    Vectors.sqdist(Vectors.dense(featuresArray), Vectors.dense(centerArray)).toFloat
  })

  val trainPredDfWithDist = trainPredDf_centroid.withColumn("dist", get_dist_udf(col("features"), col("center")))


  val top10 = trainPredDfWithDist.orderBy(col("dist").desc).limit(10).toDF()
  top10.show()


}
