import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class CreatePlotSpec extends AnyFlatSpec with Matchers{

  "CreatePlot" should "create a pie chart for attack distribution" in {

    val spark = SparkSession.builder()
      .appName("TestCreatePlotPieChart")
      .config("spark.master", "local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val createPlot = new CreatePlot

    val dfWithCategorySchema = new StructType()
      .add(name = "prediction", DoubleType, nullable = false)
      .add(name = "predictionInteger", IntegerType, nullable = false)
      .add(name = "predictionString", StringType, nullable = false)
      .add(name = "category", StringType, nullable = false)

    val outputResultPath = "src/test/resources/CreatePlotSpec_data/dfWithCategory_test.csv"

    val dfWithCategory_test = spark.read
      .format("csv")
      .option("header", true)
      .option("sep", ",")
      .schema(dfWithCategorySchema)
      .load(outputResultPath)

    val plotsBasePath = "src/test/resources/plots/"
    try
    {
      createPlot.createPieChart(dfWithCategory_test, plotsBasePath)

      // Add assertions if needed
      assert(new File(plotsBasePath + "pieChart.png").exists())
    }
    finally {
      new File(plotsBasePath, "pieChart.png").delete()
    }
  }

  "CreatePlot" should "create a bar chart for distribution of signature of attacks" in
  {
    val createPlot = new CreatePlot

    val plotsBasePath = "src/test/resources/plots/"

    try {
      createPlot.createBarChart(Seq(1.0, 2.0), Seq("first", "second") ,"test", plotsBasePath)

      // Add assertions if needed
      assert(new File(plotsBasePath + "test.png").exists())
    }
    finally {
      new File(plotsBasePath, "test.png").delete()
    }


  }




}
