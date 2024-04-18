# csye7200-project
The purpose of this project is to leverage telecom network logs to identify and flag anomalous activities, ultimately enhancing cybersecurity threat detection. 

## Methodology

We implemented the Pub/Sub infrastructure using Apache Kafka. We introduced structured streaming using Apache Spark. A read stream context is always alive, and ready to process new logs from the Apache Kafka topic. We extract features and real time prediction using Scala/Spark. We predict the anomalies using a random forest model training on the KDD 1999 Cup dataset. The model predicts the attacks and classified them into the following categories and sub-categories

1. DOS: denial-of-service, e.g. syn flood; (Teardrop, Back, Land, Smurf, Neptune, Pod)
2. R2L: unauthorized access from a remote machine, e.g. guessing password; (guess_passwd, map, spy, phd, warezmaster, wazerclient, multi hop, ftp_write)
3. U2R:  unauthorized access to local superuser (root) privileges, e.g., various ``buffer overflow'' attacks; (perl, rootkit, buffer_overflow, load module)
4. Probing: surveillance and other probing, e.g., port scanning. (Nmap, portsweep, ipsweep, satan)

## Setup

To setup download kafka and run the following commands to start zookeeper, server and create the topic
1. bin/zookeeper-server-start.sh config/zookeeper.properties
2. bin/kafka-server-start.sh config/server.properties
3. bin/kafka-topics.sh --create --topic data-stream --bootstrap-server localhost:9092


## Run Project

1. To start the streaming, run the submit-spark-assignment-1.sh script in the spark-streaming projectt
2. Run Main.scala in the kafka-producer project to input the data to the topic
3. Run the evaluator.scala program to generate and email the report to the admin