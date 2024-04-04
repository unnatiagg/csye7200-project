ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

lazy val root = (project in file("."))
  .settings(
    name := "spark-streaming"
  )

val kafkaVersion = "3.7.0"
val sparkVersion = "3.5.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // streaming
  "org.apache.spark" %% "spark-streaming" % sparkVersion,

  // streaming-kafka
  "org.apache.spark" % "spark-sql-kafka-0-10_2.13" % sparkVersion,

  // low-level integrations
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,


  // kafka
//  "org.apache.kafka" %% "kafka" % kafkaVersion,
//  "org.apache.kafka" % "kafka-streams" % kafkaVersion
)