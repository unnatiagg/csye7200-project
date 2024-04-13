import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSink

import java.util.Random
import java.util.UUID
import java.time.Instant

object StreamingDataSimulator {
  def main(args: Array[String]): Unit = {
    // Set up the Flink execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Define the schema of the streaming data
    //DataStream
    val streamData = env.generateSequence(1, 100) // Generate a sequence of integers as a placeholder

    // Generate synthetic data based on the schema
    val simulatedData = streamData.map { _ =>
      val fake = new FakeDataGenerator()
      val random = new Random()
      val timeNow = Instant.now().getEpochSecond()

      // Generate random data based on the schema
      val subscriberId = fake.uuid4()
      val srcIP = fake.ipv4()
      val dstIP = fake.ipv4()
      val srcPort = random.nextInt(65535 - 1024 + 1) + 1024
      val dstPort = random.nextInt(65535 - 1024 + 1) + 1024
      val txBytes = random.nextInt(200000 - 10000 + 1) + 10000
      val rxBytes = random.nextInt(50000 - 10000 + 1) + 10000
      val startTime = timeNow
      val endTime = timeNow
      val tcpFlag = random.nextInt(3) // Random TCP flags (0, 1, 2)
      val protocolName = Seq("tcp", "udp", "icmp")(random.nextInt(3))
      val protocolNumber = random.nextInt(255)

      s"""{"subscriberId": "$subscriberId", "srcIP": "$srcIP", "dstIP": "$dstIP", "srcPort": $srcPort, "dstPort": $dstPort, "txBytes": $txBytes, "rxBytes": $rxBytes, "startTime": $startTime, "endTime": $endTime, "tcpFlag": $tcpFlag, "protocolName": "$protocolName", "protocolNumber": $protocolNumber}"""
    }

    // Set up Pub/Sub sink options
    val pubSubSink = PubSubSink.newBuilder()
      .withSerializationSchema(new SimpleStringSchema())
      .withProjectName("csye-7200-team-4")
      .withTopicName("my-topic")
      .build()

    // Create Pub/Sub sink
   // val pubSubSink = new PubSubSink[String](pubSubSinkOptions, new SimpleStringSchema())

    // Publish the simulated data to the Pub/Sub topic
    simulatedData.addSink(pubSubSink)

    // Execute the Flink job
    env.execute("Streaming Data Simulator")
  }

  // Define a FakeDataGenerator class to generate UUIDs and IP addresses
  class FakeDataGenerator {
    def uuid4(): String = UUID.randomUUID().toString

    def ipv4(): String = {
      val random = new Random()
      s"${random.nextInt(256)}.${random.nextInt(256)}.${random.nextInt(256)}.${random.nextInt(256)}"
    }
  }
}
