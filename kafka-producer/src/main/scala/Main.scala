import java.util.{Properties, UUID}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import com.google.gson.Gson
import scala.util.Random
import scala.concurrent.duration._
import java.io.File
import scala.io.Source
import io.github.cdimascio.dotenv.Dotenv

/*
This class is used to parse the logs from the csv file and create Event Data Objects
org.apache.kafka.common.serialization.StringSerializer is used for Serializing the Event Data Objects
For the records, where the parsing was not successful they are not sent into the pipeline
 */
object KafkaDataCSV {

  /*
  Schema for the Log/event data
   */
  case class EventData(
                        duration: Int,
                        protocol_type: String,
                        service: String,
                        flag: String,
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
                        dst_host_srv_rerror_rate: Double
                      )



  /*
  Main function for parsing each record in the Log file and generating an Event Data objecy
  Each Object is then serialized using com.google.gson.Gson and org.apache.kafka.common.serialization.StringSerializer
  before publishing onto the kafka topic
   */
  def main(args: Array[String]): Unit = {
    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "localhost:9092")
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](kafkaProps)
    val dotenv = Dotenv.configure().load();
    val csvFilePath = dotenv.get("LOG_CSV_PATH")
    val topic = dotenv.get("TOPIC_NAME")
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

    }
    producer.close()
  }

  /*
  Parsing each string from the csv file into an EventData Object
   */
  def generateEventData(s: String): Option[EventData] = {

    val tokens = s.split(",").toList

    try {
      Some(EventData(
        duration = tokens(0).toInt,
        protocol_type = tokens(1),
        service = tokens(2),
        flag = tokens(3),
        src_bytes = tokens(4).toInt,
        dst_bytes = tokens(5).toInt,
        land = tokens(6).toInt,
        wrong_fragment = tokens(7).toInt,
        urgent = tokens(8).toInt,
        hot = tokens(9).toInt,
        num_failed_logins = tokens(10).toInt,
        logged_in = tokens(11).toInt,
        num_compromised = tokens(12).toInt,
        root_shell = tokens(13).toInt,
        su_attempted = tokens(14).toInt,
        num_root = tokens(15).toInt,
        num_file_creations = tokens(16).toInt,
        num_shells = tokens(17).toInt,
        num_access_files = tokens(18).toInt,
        num_outbound_cmds = tokens(19).toInt,
        is_host_login = tokens(20).toInt,
        is_guest_login = tokens(21).toInt,
        count = tokens(22).toInt,
        srv_count = tokens(23).toInt,
        serror_rate = tokens(24).toDouble,
        srv_serror_rate = tokens(25).toDouble,
        rerror_rate = tokens(26).toDouble,
        srv_rerror_rate = tokens(27).toDouble,
        same_srv_rate = tokens(28).toDouble,
        diff_srv_rate = tokens(29).toDouble,
        srv_diff_host_rate = tokens(30).toDouble,
        dst_host_count = tokens(31).toInt,
        dst_host_srv_count = tokens(32).toInt,
        dst_host_same_srv_rate = tokens(33).toDouble,
        dst_host_diff_srv_rate = tokens(34).toDouble,
        dst_host_same_src_port_rate = tokens(35).toDouble,
        dst_host_srv_diff_host_rate = tokens(36).toDouble,
        dst_host_serror_rate = tokens(37).toDouble,
        dst_host_srv_serror_rate = tokens(38).toDouble,
        dst_host_rerror_rate = tokens(39).toDouble,
        dst_host_srv_rerror_rate = tokens(40).toDouble
      ))
    } catch {
      case _: NumberFormatException => None
    }
  }


}
