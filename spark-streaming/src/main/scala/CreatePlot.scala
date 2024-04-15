import com.cibo.evilplot.plot.{BarChart, PieChart}
import com.cibo.evilplot.demo.DemoPlots.{areaPlot, theme}
import org.apache.spark.sql.DataFrame

import java.io.File

class CreatePlot {


  /*
  This method is used to create a Pie Chart for attack distribution by category as predicted by our model
  The 5 categories: NORMAL, PROBE, DOS, R2L, U2L
   */
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

    /*
    We want to save these plots to accumulate them later while sending the detailed report
     */

    val outputFile = new File("src/resources/result/plots/"+outputFileName)
    pieplot.write(outputFile)

  }

  /*
  This method show the sub-categories of each of the attack type
  We display which sub-categories belong to each attack type
  What is the count distribution of each of the sub category
   */
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
