

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, explode, from_json}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

object SparkStreamingNetworkData {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkStreaming")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "data-stream")
      .option("startingOffsets", "latest")
      .load()

    import org.apache.spark.sql.types._

    val eventDataSchema = StructType(Seq(
      StructField("duration", FloatType, nullable = true),
      StructField("src_bytes", FloatType, nullable = true),
      StructField("dst_bytes", FloatType, nullable = true),
      StructField("land", FloatType, nullable = true),
      StructField("wrong_fragment", FloatType, nullable = true),
      StructField("urgent", FloatType, nullable = true),
      StructField("hot", FloatType, nullable = true),
      StructField("num_failed_logins", FloatType, nullable = true),
      StructField("logged_in", FloatType, nullable = true),
      StructField("num_compromised", FloatType, nullable = true),
      StructField("root_shell", FloatType, nullable = true),
      StructField("su_attempted", FloatType, nullable = true),
      StructField("num_root", FloatType, nullable = true),
      StructField("num_file_creations", FloatType, nullable = true),
      StructField("num_shells", FloatType, nullable = true),
      StructField("num_access_files", FloatType, nullable = true),
      StructField("num_outbound_cmds", FloatType, nullable = true),
      StructField("is_host_login", FloatType, nullable = true),
      StructField("is_guest_login", FloatType, nullable = true),
      StructField("count", FloatType, nullable = true),
      StructField("srv_count", FloatType, nullable = true),
      StructField("serror_rate", FloatType, nullable = true),
      StructField("srv_serror_rate", FloatType, nullable = true),
      StructField("rerror_rate", FloatType, nullable = true),
      StructField("srv_rerror_rate", FloatType, nullable = true),
      StructField("same_srv_rate", FloatType, nullable = true),
      StructField("diff_srv_rate", FloatType, nullable = true),
      StructField("srv_diff_host_rate", FloatType, nullable = true),
      StructField("dst_host_count", FloatType, nullable = true),
      StructField("dst_host_srv_count", FloatType, nullable = true),
      StructField("dst_host_same_srv_rate", FloatType, nullable = true),
      StructField("dst_host_diff_srv_rate", FloatType, nullable = true),
      StructField("dst_host_same_src_port_rate", FloatType, nullable = true),
      StructField("dst_host_srv_diff_host_rate", FloatType, nullable = true),
      StructField("dst_host_serror_rate", FloatType, nullable = true),
      StructField("dst_host_srv_serror_rate", FloatType, nullable = true),
      StructField("dst_host_rerror_rate", FloatType, nullable = true),
      StructField("dst_host_srv_rerror_rate", FloatType, nullable = true)
    ))


    //val songs = df.selectExpr("cast (value as string)").select(from_json(col("value"),eventDataSchema).as("data")).select("data.*")

   // val songExpanded = songs.select(explode(col("songDetails").as(Seq("trackName", "lyrics"))))

    val songs = df.selectExpr("cast(value as string)")
      .select(from_json(col("value"), eventDataSchema).as("data"))
      .select("data.*")

    val q = songs.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false) // To avoid truncating the output
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val count = batchDF.count()
        println(s"Batch ID: $batchId, Count: $count")
      }
      .start()

    q.awaitTermination()

  }
}