ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

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
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,

  // low-level integrations
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion ,

  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.sendgrid" % "sendgrid-java" % "4.10.2",
  "io.github.cdimascio" % "dotenv-java" % "3.0.0",
  "io.github.cibotech" %% "evilplot" % "0.9.0"
  // kafka
//  "org.apache.kafka" %% "kafka" % kafkaVersion,
//  "org.apache.kafka" % "kafka-streams" % kafkaVersion
)

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF","services",xs @ _*) => MergeStrategy.filterDistinctLines
  case PathList("META-INF",xs @ _*) => MergeStrategy.discard
  case "application.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}
