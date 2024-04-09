//
//import java.util
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object DataProcessing {
//
//  //Deserializer means taking the json object and converting it into DSSTREAMS
//
//
//
//
//
//
//
//  def readFromKafka() = {
//
//
//    val spark = SparkSession.builder()
//      .appName("Spark DStreams + Kafka")
//      .master("local[2]")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    val kafkaTopic = "data-stream"
//
//
//    //Create the streaming_df to read from kafka
//    val streaming_df = spark.readStream
//    .format("kafka")
//    .option("kafka.bootstrap.servers", "localhost:9092")
//    .option("subscribe", "data-stream")
//    .load()
//
//    val printdf = streaming_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//      .as[(String, String)]
//
//    printdf.show()
//
//  }
//
//
//
//  def main(args: Array[String]): Unit = {
//    readFromKafka()
//  }
//
//}