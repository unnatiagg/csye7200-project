import java.util.{Properties, UUID}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import com.google.gson.Gson
import scala.util.Random
import scala.concurrent.duration._
import io.github.cdimascio.dotenv.Dotenv

/*
This is used to generate fake data based on the Event Data Schema
The Event Data is published onto the Kafka Topics
 */
object KafkaDataGenerator {
  case class EventData(subscriberId: String, srcIP: String, dstIP: String, srcPort: Int,
                       dstPort: Int, txBytes: Int, rxBytes: Int, startTime: Long,
                       endTime: Long, tcpFlag: Int, protocolName: String, protocolNumber: Int)

  def main(args: Array[String]): Unit = {


    val dotenv = Dotenv.configure().load();
    val topic = dotenv.get("TOPIC_NAME")
    val faker = new scala.util.Random
    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "localhost:9092")
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](kafkaProps)

    while (true) {
      val event = generateEventData(faker)
      val message = new ProducerRecord[String, String](topic, UUID.randomUUID().toString, new Gson().toJson(event))
      producer.send(message)
      println(s"Published message: $message")

      //generating one record per second
      //batch size of 350000
      val sleepTimeMillis = (0.5.second.toMillis) //+ Random.nextInt(5).seconds.toMillis).toInt
      Thread.sleep(sleepTimeMillis)

    }

    producer.close()
  }

  def generateEventData(random: Random): EventData = {
    val subscriberId = UUID.randomUUID().toString
    val srcIP = random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256)
    val dstIP = random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256)
    val srcPort = 1024 + random.nextInt(64511)
    val dstPort = 1024 + random.nextInt(64511)
    val txBytes = if (random.nextDouble() < 0.05) Random.nextInt(500000) + 500000 else Random.nextInt(190001) + 10000
    val rxBytes = if (random.nextDouble() < 0.05) Random.nextInt(500000) + 500000 else Random.nextInt(40001) + 10000
    val startTime = System.currentTimeMillis() / 1000
    val endTime = System.currentTimeMillis() / 1000
    val tcpFlag = random.nextInt(3)
    val protocolName = if (random.nextDouble() < 0.33) "tcp" else if (random.nextDouble() < 0.66) "udp" else "icmp"
    val protocolNumber = random.nextInt(256)

    EventData(subscriberId, srcIP, dstIP, srcPort, dstPort, txBytes, rxBytes,
      startTime, endTime, tcpFlag, protocolName, protocolNumber)
  }
}
