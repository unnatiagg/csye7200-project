ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.6.1"
libraryDependencies += "com.google.code.gson" % "gson" % "2.10.1"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-producer"
  )