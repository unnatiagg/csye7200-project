ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"
libraryDependencies += "org.apache.flink" % "flink-connector-gcp-pubsub" % "1.16.3"
libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.14.6"
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.14.6"
libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.14.6"


lazy val root = (project in file("."))
  .settings(
    name := "apache-flink-datasource"
  )
