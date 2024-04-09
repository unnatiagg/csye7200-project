//
//import java.util
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object IntegratingKafkaDStreams extends App{
//
//  //Deserializer means taking the json object and converting it into DSSTREAMS
//  val spark = SparkSession.builder()
//    .appName("Spark DStreams + Kafka")
//    .master("local[2]")
//    .getOrCreate()
//
//  import spark.implicits._
//
//  val ssc = new StreamingContext(spark.sparkContext, Seconds(300))
//
//  val kafkaParams: Map[String, Object] = Map(
//    "bootstrap.servers" -> "localhost:9092",
//    "key.serializer" -> classOf[StringSerializer], // send data to kafka
//    "value.serializer" -> classOf[StringSerializer],
//    "key.deserializer" -> classOf[StringDeserializer], // receiving data from kafka
//    "value.deserializer" -> classOf[StringDeserializer],
//    "auto.offset.reset" -> "latest",
//    "enable.auto.commit" -> false.asInstanceOf[Object]
//  )
//
//  val kafkaTopic = "data-stream"
//
//  //def readFromKafka() = {
//    val topics = Array(kafkaTopic)
//    val kafkaDStream = KafkaUtils.createDirectStream(
//      ssc,
//      LocationStrategies.PreferConsistent,
//      /*
//       Distributes the partitions evenly across the Spark cluster.
//       Alternatives:
//       - PreferBrokers if the brokers and executors are in the same cluster
//       - PreferFixed
//      */
//      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
//      /*
//        Alternative
//        - SubscribePattern allows subscribing to topics matching a pattern
//        - Assign - advanced; allows specifying offsets and partitions per topic
//       */
//    )
//
//    /*
//    Printing the count of each processed Stream
//     */
//    //val processedStream = kafkaDStream.map(record => (record.key(), record.value()))
//
//    val countPeople: DStream[Long] = kafkaDStream.count()
//    countPeople.print()
//
//
// //kafkaDStream.foreachRDD()
//  kafkaDStream.foreachRDD { rdd =>
//    // Extract "value" field from each record in RDD
//    val valueRDD = rdd.map(_.value()) // Assuming JSON string is stored in "value" field
//
//    // Create DataFrame from RDD and schema
//    val valueDF = spark.read.schema(valueSchema).json(valueRDD)
//
//    // Perform model prediction on the "value" DataFrame
//    val predictions = model.transform(valueDF) // Assuming model is a transformer
//
//    // Handle predictions (e.g., store, display, etc.)
//    predictions.show()
//  }
//
//
//    ssc.start()
//    ssc.awaitTermination()
//
//  def writeToKafka() = {
//    val inputData = ssc.socketTextStream("localhost", 12345)
//
//    // transform data
//    val processedData = inputData.map(_.toUpperCase())
//
//    processedData.foreachRDD { rdd =>
//      rdd.foreachPartition { partition =>
//        // inside this lambda, the code is run by a single executor
//
//        val kafkaHashMap = new util.HashMap[String, Object]()
//        kafkaParams.foreach { pair =>
//          kafkaHashMap.put(pair._1, pair._2)
//        }
//
//        // producer can insert records into the Kafka topics
//        // available on this executor
//        val producer = new KafkaProducer[String, String](kafkaHashMap)
//
//        partition.foreach { value =>
//          val message = new ProducerRecord[String, String](kafkaTopic, null, value)
//          // feed the message into the Kafka topic
//          producer.send(message)
//        }
//
//        producer.close()
//      }
//    }
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
////  def main(args: Array[String]): Unit = {
////    readFromKafka()
////  }
//
//}