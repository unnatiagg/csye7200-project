import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object ReadAndSplitCSV {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder
      .appName("Read and Split CSV")
      .config("spark.master", "local")
      .getOrCreate()

    // Read CSV file into DataFrame
    val df = spark.read
      //.option("header", "true")
      .csv("src/resources/data/Test.csv")

    // Drop last two columns
    val columnsToDrop = df.columns.takeRight(2)
    val dfWithoutLastTwoColumns = df.drop(columnsToDrop:_*)

    // Get number of rows
    val totalRows = dfWithoutLastTwoColumns.count()

    // Split DataFrame into two halves
    val df1 = dfWithoutLastTwoColumns.limit((totalRows / 2).toInt)
    val df2 = dfWithoutLastTwoColumns.except(df1)

    // Save each half into separate CSV files
    df1.write
      //.option("header", "true")
      .mode("overwrite")
      .csv("src/resources/data/logFile1.csv")

    df2.write
     // .option("header", "true")
      .mode("overwrite")
      .csv("src/resources/data/logFile2.csv")

    // Stop SparkSession
    spark.stop()
  }
}
