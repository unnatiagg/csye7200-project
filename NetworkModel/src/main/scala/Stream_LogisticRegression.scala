import LogisticRegression.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}


object Stream_LogisticRegression extends App {

  val spark = SparkSession.builder()
    .appName("LogisticRegression")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR") // We want to ignore all of the INFO and WARN messages.

  val schema = StructType(
    Array(
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
      StructField("dst_host_srv_rerror_rate", FloatType, nullable = true),
      StructField("status", StringType, nullable = true)
    )
  )

  val df = spark.read
    .format("csv")
    .option("header", true)
    .option("sep", ",")
    .schema(schema)
    .load("data")

  df.printSchema()







}
