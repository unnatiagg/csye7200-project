import com.sendgrid.helpers.mail.Mail
import com.sendgrid.helpers.mail.objects.{Content, Email}
import com.sendgrid.{Method, Request, Response, SendGrid}
import io.github.cdimascio.dotenv.Dotenv

import java.nio.file.{Files, Paths}
import scala.util.{Failure, Success, Try}

class SendEmailReport {

  val dotenv = Dotenv.configure().load();
  val SENDGRID_API_KEY = dotenv.get("SENDGRID_API_KEY")
  val sg = new SendGrid(SENDGRID_API_KEY);
  val sender = new Email("networkreport@em6774.sachdevacloud.com")

  def sendEmail(to: String, attackPercent: Double, plotsPath: String) = {

    val receiver = new Email(to)
    val subject = "Network Data Report"

    // Load the image file and convert it to base64
    val pieChartData = Files.readAllBytes(Paths.get(s"$plotsPath/pieChart.png"))
    val pieChartAttachment = java.util.Base64.getEncoder.encodeToString(pieChartData)

    val attackTypes = Seq("probe", "dos", "r2l", "u2r")

    val attackDescriptions = Map(
      "dos" -> "denial-of-service, e.g. syn flood",
      "r2l" -> "unauthorized access from a remote machine, e.g. guessing password",
      "u2r" -> "unauthorized access to local superuser (root) privileges, e.g., various 'buffer overflow' attacks",
      "probe" -> "surveillance and other probing, e.g., port scanning"
    )

    // Create <img> tags for each attack type
    val attackImages = attackTypes.map { attackType =>
      val imagePath = s"$plotsPath/$attackType.png"
      val imageData = Files.readAllBytes(Paths.get(imagePath))
      val imageBase64 = java.util.Base64.getEncoder.encodeToString(imageData)
      s"""
         |<h4>The total distribution of signature of $attackType : ${attackDescriptions.get(attackType)match {
        case Some(str) => str
        case None => ""
      }}</h4>
         |<img src="data:image/png;base64,$imageBase64" alt="$attackType.png" style="max-width:40%;height:auto;">
         |""".stripMargin
    }.mkString("\n")


    val contentString: String =
      s"""
         |<html>
         |<body>
         |<h2>Hi Network Admin,</h2>
         |<h3>Total percentage of attacks detected in the network feed are ${"%.2f".format(attackPercent)}%</h3>
         |<h4>The total distribution of feed:</h4>
         |<img src="data:image/png;base64,$pieChartAttachment" alt="pie_chart.png" style="max-width:40%;height:auto;">
         |$attackImages
         |</body>
         |</html>
         |""".stripMargin

    // Save the HTML content to a file
    val htmlFilePath = "email_template.html"
    Files.write(Paths.get(htmlFilePath), contentString.getBytes())



    val content = new Content()
    content.setType("text/html")
    content.setValue(contentString)
    val email = new Mail(sender, subject, receiver, content)

    val result: Try[Response] = Try {
      val request = new Request()
      request.setMethod(Method.POST)
      request.setEndpoint("mail/send")
      request.setBody(email.build())
      sg.api(request)
    }

    result match {
      case Success(response) =>
        println(response.getStatusCode())
        println(response.getBody())
        println(response.getHeaders())
      case Failure(exception) =>
        println("An error occurred: " + exception.getMessage)
    }


  }








}
