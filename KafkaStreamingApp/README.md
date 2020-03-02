# Kafka Streaming App

This is a streaming application that cleans the input data based on a set of predefined rules. It also calculates some measures

## Design Approach

This application is built using kafka as the message broker and pyspark for stream processing. This application is divided in following modules

- Kafka Producer : reads the given input json file and pushes the messages on the kafka topic. Written in python using kafka-python library
- Kafka Consumer : consumes the streamed message from the kafka topic, cleans the data, saves the cleansed data in parquet format and pushes the grouped data to another topic which is further read by a kafka batch consumer to calculate the measures 
- Kafka Reconciliation/Monitoring : reconciliation module reconciles the message count between producer and consumer. Monitoring can be done from the jmx utility available in kafka using MBEAN

shellscripts/batscripts are the main entry point of individual module

## Installation

#### Applicaton installion
- Install the kafka_2.12-2.4.0 ,download from https://kafka.apache.org/quickstart
- Start the zookeeper,kafka-server and topics 
```shell script
$kafka_2.12-2.4.0>bin/zookeeper-server-start.sh config/zookeeper.properties
$kafka_2.12-2.4.0>bin/kafka-server-start.sh config/server.properties
$kafka_2.12-2.4.0>bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test_input
$kafka_2.12-2.4.0>bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test_staging
$kafka_2.12-2.4.0>bin/kafka-topics.sh --list --bootstrap-server localhost:9092
``` 
- Install kafka-python library in the python shell
```shell script
$>pip install kafka-python
```
- Install pyspark if not installed
```shell script
$>pip install pyspark
```
- Checkout the KafkaStreamingApp project from github
- Update the following variables in KafkaStreamingApp/config, an example is given below
```shell script
APP_HOME="F:/KafkaStreamingApp"
KAFKA_HOME="F:/kafka_2.12-2.4.0"
```
- Execute below command from the root folder of the project to build the egg file
```shell script
$KafkaStreamingApp>python setup.py bdist_egg
```
- Update the generated egg file name(KafkaStreamingApp-1.0.0-py3.7.egg) in the KafkaStreamingApp/config/script_conf (py3.7 python version can differ based on installation)
```shell script
EGG_FILE_NAME=KafkaStreamingApp-1.0.0-py3.7.egg 
```
- Navigate to the KafkaStreamingApp/shellscripts folder(batscripts in case of windows) in the root directory of the project
```shell script
$KafkaStreamingApp>cd shellscripts
```
- Run the execute_stream_producer.sh(.bat in case of windows) file using below command to start producer
```shell script
$KafkaStreamingApp/shellscripts>sh execute_stream_producer.sh
```
- In a new shell window run the execute_stream_consumer.sh(.bat in case of windows) file using below command to start consumer
```shell script
$KafkaStreamingApp/shellscripts>sh execute_stream_consumer.sh
```
- In a new shell window run the execute_batch_consumer.sh(.bat in case of windows) file using below command to start consumer
```shell script
$KafkaStreamingApp/shellscripts>sh execute_batch_consumer.sh
```
#### Monitoring installion
- Update the kafka-server.bat or kafka-server.sh for the jmx port 
```shell script
export JMX_PORT=9999
```
- Navigate to the KafkaStreamingApp/shellscripts folder(batscripts in case of windows) in the root directory of the project
- Run the execute_prod_cons_recon.sh(.bat in case of windows) to get the status of reconciliation between producer and consumer
- Run the execute_kafka_runclass.sh(.bat in case of windows) to get metrics based on MBEAN specified in KafkaStreamingApp/config/script_conf. 
```shell script
MBEAN="kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec"
```
###Testing
- Execute the a console consumer to check what producer is pushing to the test_input topic
```shell script
$kafka_2.12-2.4.0>bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --topic test_input
```
-Execute the a console consumer to check what stream_consumer is pushing to the test_staging topic
```shell script
$kafka_2.12-2.4.0>bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --topic test_staging
```
## Assumptions

  - This project is built in Python 3.7 and pyspark2.4.0 so assuming python is already installed
  - All the three modules have been bundled in same project with separate shell scripts to execute
  - Application uses the same IP and PORT as the kafka server was configured, which can be changed in the config/script_conf according to our needs
  - Egg file will be created before starting the application
  - All the configurations are based on local execution of kafka and spark, which can be changed based on the environment setup 
  - sqllite db (which comes with python) was used just to keep the counter of the producer messages which is used later for producer consumer reconciliation
  - The struct schema has been constructed based on the input JSON format
  - The measures have been calculated in the batch consumer and results are stored as csv file format also based on the needs this can be updated
  - Testing was done using console consumers and checking the saved results in the form of files 
  - This application was built and tested on windows platform but build has been delivered to run on linux environment
  - There are shellscripts and batscripts folders, with their specific README.md
  - Kafka version 2.12-2.4.0 used is specific to this project