import java.util.{Properties, UUID}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import com.google.gson.Gson
import scala.util.Random
import scala.concurrent.duration._
import java.io.File
import scala.io.Source

object KafkaDataCSV {
  case class EventData(
                          duration: Int,
                          srcBytes: Int,
                          dstBytes: Int,
                          land: Int,
                          wrongFragment: Int,
                          urgent: Int,
                          hot: Int,
                          numFailedLogins: Float,
                          loggedIn: Int,
                          numCompromised: Int,
                          rootShell: Int,
                          suAttempted: Int,
                          numRoot: Int,
                          numFileCreations: Int,
                          numShells: Int,
                          numAccessFiles: Int,
                          numOutboundCmds: Int,
                          isHostLogin: Int,
                          isGuestLogin: Int,
                          count: Int,
                          srvCount: Int,
                          serrorRate: Double,
                          srvSerrorRate: Double,
                          rerrorRate: Double,
                          srvRerrorRate: Double,
                          sameSrvRate: Double,
                          diffSrvRate: Double,
                          srvDiffHostRate: Double,
                          dstHostCount: Int,
                          dstHostSrvCount: Int,
                          dstHostSameSrvRate: Double,
                          dstHostDiffSrvRate: Double,
                          dstHostSameSrcPortRate: Double,
                          dstHostSrvDiffHostRate: Double,
                          dstHostSerrorRate: Double,
                          dstHostSrvSerrorRate: Double,
                          dstHostRerrorRate: Double,
                          dstHostSrvRerrorRate: Double
                          //status: String
                        )


  def main(args: Array[String]): Unit = {
    //val faker = new scala.util.Random
    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "localhost:9092")
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](kafkaProps)
    val csvFilePath = "src/main/resources/TestDf1.csv"
    val topic = "data-stream"
    val csvSource = Source.fromFile(csvFilePath)
    val csvLines = csvSource.getLines().toList

    csvLines.foreach { data =>

      val event =  generateEventData(data)

      event match {

        case Some(event) => {
          val message = new ProducerRecord[String, String](topic,UUID.randomUUID().toString, new Gson().toJson(event))
          producer.send(message)
          println(s"Published message: $message")
        }
        case None => println(s"This record cannot be parsed")
      }
      //key value
    }
    producer.close()
  }

  def generateEventData(s: String): Option[EventData] = {

    val tokens = s.split(",").toList

    try {
      Some(EventData(
        duration = tokens(0).toInt,
        srcBytes = tokens(1).toInt,
        dstBytes = tokens(2).toInt,
        land = tokens(3).toInt,
        wrongFragment = tokens(4).toInt,
        urgent = tokens(5).toInt,
        hot = tokens(6).toInt,
        numFailedLogins = tokens(7).toInt,
        loggedIn = tokens(8).toInt,
        numCompromised = tokens(9).toInt,
        rootShell = tokens(10).toInt,
        suAttempted = tokens(11).toInt,
        numRoot = tokens(12).toInt,
        numFileCreations = tokens(13).toInt,
        numShells = tokens(14).toInt,
        numAccessFiles = tokens(15).toInt,
        numOutboundCmds = tokens(16).toInt,
        isHostLogin = tokens(17).toInt,
        isGuestLogin = tokens(18).toInt,
        count = tokens(19).toInt,
        srvCount = tokens(20).toInt,
        serrorRate = tokens(21).toDouble,
        srvSerrorRate = tokens(22).toDouble,
        rerrorRate = tokens(23).toDouble,
        srvRerrorRate = tokens(24).toDouble,
        sameSrvRate = tokens(25).toDouble,
        diffSrvRate = tokens(26).toDouble,
        srvDiffHostRate = tokens(27).toDouble,
        dstHostCount = tokens(28).toInt,
        dstHostSrvCount = tokens(29).toInt,
        dstHostSameSrvRate = tokens(30).toDouble,
        dstHostDiffSrvRate = tokens(31).toDouble,
        dstHostSameSrcPortRate = tokens(32).toDouble,
        dstHostSrvDiffHostRate = tokens(33).toDouble,
        dstHostSerrorRate = tokens(34).toDouble,
        dstHostSrvSerrorRate = tokens(35).toDouble,
        dstHostRerrorRate = tokens(36).toDouble,
        dstHostSrvRerrorRate = tokens(37).toDouble,
        //status = tokens(38)
      ))
    } catch {
      case _: NumberFormatException => None
    }
  }


}
