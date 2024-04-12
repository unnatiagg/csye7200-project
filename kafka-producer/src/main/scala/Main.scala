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
                        src_bytes: Int,
                        dst_bytes: Int,
                        land: Int,
                        wrong_fragment: Int,
                        urgent: Int,
                        hot: Int,
                        num_failed_logins: Int,
                        logged_in: Int,
                        num_compromised: Int,
                        root_shell: Int,
                        su_attempted: Int,
                        num_root: Int,
                        num_file_creations: Int,
                        num_shells: Int,
                        num_access_files: Int,
                        num_outbound_cmds: Int,
                        is_host_login: Int,
                        is_guest_login: Int,
                        count: Int,
                        srv_count: Int,
                        serror_rate: Double,
                        srv_serror_rate: Double,
                        rerror_rate: Double,
                        srv_rerror_rate: Double,
                        same_srv_rate: Double,
                        diff_srv_rate: Double,
                        srv_diff_host_rate: Double,
                        dst_host_count: Int,
                        dst_host_srv_count: Int,
                        dst_host_same_srv_rate: Double,
                        dst_host_diff_srv_rate: Double,
                        dst_host_same_src_port_rate: Double,
                        dst_host_srv_diff_host_rate: Double,
                        dst_host_serror_rate: Double,
                        dst_host_srv_serror_rate: Double,
                        dst_host_rerror_rate: Double,
                        dst_host_srv_rerror_rate: Double,
                        status: String
                      )



  def main(args: Array[String]): Unit = {
    //val faker = new scala.util.Random
    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "localhost:9092")
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](kafkaProps)
    val csvFilePath = "src/main/resources/TrainDf.csv"
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
        src_bytes = tokens(1).toInt,
        dst_bytes = tokens(2).toInt,
        land = tokens(3).toInt,
        wrong_fragment = tokens(4).toInt,
        urgent = tokens(5).toInt,
        hot = tokens(6).toInt,
        num_failed_logins = tokens(7).toInt,
        logged_in = tokens(8).toInt,
        num_compromised = tokens(9).toInt,
        root_shell = tokens(10).toInt,
        su_attempted = tokens(11).toInt,
        num_root = tokens(12).toInt,
        num_file_creations = tokens(13).toInt,
        num_shells = tokens(14).toInt,
        num_access_files = tokens(15).toInt,
        num_outbound_cmds = tokens(16).toInt,
        is_host_login = tokens(17).toInt,
        is_guest_login = tokens(18).toInt,
        count = tokens(19).toInt,
        srv_count = tokens(20).toInt,
        serror_rate = tokens(21).toDouble,
        srv_serror_rate = tokens(22).toDouble,
        rerror_rate = tokens(23).toDouble,
        srv_rerror_rate = tokens(24).toDouble,
        same_srv_rate = tokens(25).toDouble,
        diff_srv_rate = tokens(26).toDouble,
        srv_diff_host_rate = tokens(27).toDouble,
        dst_host_count = tokens(28).toInt,
        dst_host_srv_count = tokens(29).toInt,
        dst_host_same_srv_rate = tokens(30).toDouble,
        dst_host_diff_srv_rate = tokens(31).toDouble,
        dst_host_same_src_port_rate = tokens(32).toDouble,
        dst_host_srv_diff_host_rate = tokens(33).toDouble,
        dst_host_serror_rate = tokens(34).toDouble,
        dst_host_srv_serror_rate = tokens(35).toDouble,
        dst_host_rerror_rate = tokens(36).toDouble,
        dst_host_srv_rerror_rate = tokens(37).toDouble,
        status = tokens(38).toString
      ))
    } catch {
      case _: NumberFormatException => None
    }
  }


}
