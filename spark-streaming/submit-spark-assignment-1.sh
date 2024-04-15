#!/bin/bash

echo "Compiling and assembling application..."
sbt assembly
SPARK_HOME=/opt/homebrew/Cellar/apache-spark/3.5.0
JARFILE=`pwd`/target/scala-2.12/spark-streaming-assembly-0.1.0-SNAPSHOT.jar

# Run it locally
${SPARK_HOME}/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --class SparkStreamingNetworkData --master local $JARFILE
