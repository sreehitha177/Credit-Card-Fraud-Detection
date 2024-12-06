## Prerequisites

- Apache Kafka: Download and install Apache Kafka on your local machine or a server. You can find the Kafka downloads and installation instructions on the [Apache Kafka website](https://kafka.apache.org/downloads).
- Python: Make sure you have Python installed on your system.

## Setup and Usage -python virtual environment
- clone the repository
1. download and extract kafka
2. build the kafka project -inside the kafka folder run ```./gradlew jar -PscalaVersion=2.13.14```
3. Start ZooKeeper: ```bin/zookeeper-server-start.sh config/zookeeper.properties```
4. start kafka brokers: ```bin/kafka-server-start.sh config/server.properties```
5. install kafka-python
6. create kafka topics:
    ```bin/kafka-topics.sh --create --topic task_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1```
    ```bin/kafka-topics.sh --create --topic result_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1```
7. run ```consumer.py``` and ```producer.py``` in separate terminals
8. Verify Output: The consumer will process the tasks produced by the producer and print the results to the console.

# Consumer 
1. ```spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4 consumer.py```