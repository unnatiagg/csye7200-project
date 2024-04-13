//import org.apache.spark.sql.{SparkSession, DataFrame}
//import org.apache.spark.ml.clustering.GaussianMixture
//import org.apache.spark.ml.feature.VectorAssembler
//import org.apache.spark.sql.functions.col
//import org.apache.spark.sql.functions.udf
//import org.apache.spark.sql.functions.rand
//import org.apache.spark.sql.types.{StructType, StructField, DoubleType}
//
//object SyntheticDataGeneration {
//  def main(args: Array[String]): Unit = {
//    // Create SparkSession
//    val spark = SparkSession.builder()
//      .appName("Synthetic Data Generation")
//      .getOrCreate()
//
//    // Load data from CSV
//    val data: DataFrame = spark.read.format("csv")
//      .option("header", "true")
//      .load("src/main/resources/Test.csv")
//
//    // Separate numerical and categorical columns
//    val (numericalColumns, categoricalColumns) = separateColumns(data)
//
//    // Generate synthetic data using GMM for numerical columns
//    val syntheticNumericalData = generateSyntheticNumericalData(data, numericalColumns)
//
//    // Generate synthetic data using Copula-based method for categorical columns
//    val syntheticCategoricalData = generateSyntheticCategoricalData(data, categoricalColumns)
//
//    // Combine synthetic numerical and categorical data
//    val syntheticData = combineSyntheticData(syntheticNumericalData, syntheticCategoricalData)
//
//    // Write synthetic data to CSV
//    syntheticData.write.format("csv").option("header", "true").save("path/to/save/synthetic_data.csv")
//
//    // Stop SparkSession
//    spark.stop()
//  }
//
//  // Function to separate numerical and categorical columns
//  def separateColumns(data: DataFrame): (Array[String], Array[String]) = {
//    val numericalColumns = data.dtypes.filter(_._2 == "IntegerType" || _._2 == "DoubleType").map(_._1)
//    val categoricalColumns = data.dtypes.filter(_._2 == "StringType").map(_._1)
//    (numericalColumns, categoricalColumns)
//  }
//
//  // Function to generate synthetic data using GMM for numerical columns
//  def generateSyntheticNumericalData(data: DataFrame, numericalColumns: Array[String]): DataFrame = {
//    // Convert numerical data to feature vector
//    val assembler = new VectorAssembler()
//      .setInputCols(numericalColumns)
//      .setOutputCol("features")
//    val numericalFeatureVector = assembler.transform(data)
//
//    // Fit Gaussian Mixture Model (GMM)
//    val gmm = new GaussianMixture()
//      .setK(10) // Number of clusters
//      .setSeed(123)
//      .fit(numericalFeatureVector)
//
//    // Generate synthetic data using GMM
//    val syntheticNumericalData = gmm.sample(100000, true).select("features").toDF(numericalColumns: _*)
//
//    syntheticNumericalData
//  }
//
//  // Function to generate synthetic data using Copula-based method for categorical columns
//  def generateSyntheticCategoricalData(data: DataFrame, categoricalColumns: Array[String]): DataFrame = {
//    // Convert categorical columns to numerical representation using one-hot encoding
//    val oneHotEncodedData = categoricalColumns.foldLeft(data)((df, colName) => {
//      val distinctValues = df.select(col(colName)).distinct().rdd.map(r => r(0).toString).collect()
//      val mapping = distinctValues.zipWithIndex.toMap
//      val mappingUdf = udf((value: String) => mapping(value))
//      df.withColumn(colName + "_index", mappingUdf(col(colName)))
//    })
//
//    // Generate random uniform samples for each categorical column index
//    val uniformSamples = oneHotEncodedData.select(categoricalColumns.map(col(_ + "_index")): _*)
//      .withColumn("rand", rand(seed = 123)) // Add random uniform value column
//
//    // Convert random uniform samples to standard normal using inverse cumulative distribution function (CDF)
//    val cdfUdf = udf((rand: Double) => math.sqrt(2) * math.erfInv(2 * rand - 1))
//    val standardNormalSamples = uniformSamples.select(uniformSamples.columns.map(cdfUdf(_)): _*)
//
//    // Convert standard normal samples to the desired Copula distribution (e.g., Gaussian Copula)
//    // For simplicity, here we assume Gaussian Copula for demonstration purposes
//    val syntheticCategoricalData = standardNormalSamples
//
//    syntheticCategoricalData
//  }
//
//  // Function to combine synthetic numerical and categorical data
//  def combineSyntheticData(syntheticNumericalData: DataFrame, syntheticCategoricalData: DataFrame): DataFrame = {
//    syntheticNumericalData.crossJoin(syntheticCategoricalData) // Cartesian product to combine both datasets
//  }
//}
