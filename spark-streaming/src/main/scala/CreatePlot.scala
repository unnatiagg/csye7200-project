import com.cibo.evilplot.plot.{BarChart, PieChart}
import com.cibo.evilplot.demo.DemoPlots.{areaPlot, theme}
import org.apache.spark.sql.DataFrame

import java.io.File

class CreatePlot {

  def createPieChart(dfWithCategory: DataFrame, outputFileName: String) = {

    val groupeddf = dfWithCategory.groupBy("category").count()
    val seqDFCat = groupeddf.collect().map(arr => (arr(0).toString, arr(1).toString.toDouble)).toSeq

    val attackTypes = Seq("probe", "dos", "r2l", "u2r", "normal")

    val attachTypesCount = attackTypes.map(attack => seqDFCat.find(_._1 ==attack)match {
      case Some(value) => ""-> value._2
      case None => "" -> 0.0
    })


    val pieplot = PieChart(attachTypesCount)
      .rightLegend(labels = Some(attackTypes))
      .render()

    val outputFile = new File("src/resources/result/plots/"+outputFileName)
    pieplot.write(outputFile)

  }

  def createBarChart(counts: Seq[Double], labels: Seq[String], attack: String) = {
    val plot = BarChart(counts)
      .standard(xLabels = labels)
      .xLabel("Attack")
      .yLabel("Count")
      .render()

    val outputFile = new File("src/resources/result/plots/" + attack + ".png")
    plot.write(outputFile)
  }




}
