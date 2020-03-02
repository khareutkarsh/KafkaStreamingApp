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
- Checkout the kafka_2.12-2.4.0 folder from the github or download from https://kafka.apache.org/quickstart
- Start the zookeeper,kafka-server and topics based on the README.md in the checked out kafka_2.12-2.4.0 folder 
- Install kafka-python library in the python shell
```shell script
$>pip install kafka-python
```
- Checkout the KafkaStreamingApp project from github
- Update the KAFKA_HOME variable in KafkaStreamingApp/pyscripts/constants/app_consumer_constants.py
```python
KAFKA_HOME="F:\kafka_2.12-2.4.0"
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
- Update the kafka-server.bat or kafka-server.sh for the jmx port as given in the README.md of kafka folder
- Navigate to the KafkaStreamingApp/shellscripts folder(batscripts in case of windows) in the root directory of the project
- Run the execute_prod_cons_recon.sh(.bat in case of windows) to get the status of reconciliation between producer and consumer
- Run the execute_kafka_jmx_monitoring.sh(.bat in case of windows) to get metrics based on MBEAN specified in KafkaStreamingApp/pyscripts/constants/app_consumer_constants.py. Should be done before making the egg file otherwise have to create the egg file again
```python
MBEAN="kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec"
```

## Assumptions

  - This project is built in Python 3.7 and pyspark2.4.0 so assuming python is already installed
  - All the three modules have been bundled in same project with separate shell scripts to execute
  - Application uses the same IP and PORT as the kafka server was configured, which can be changed in the constants according to our needs
  - Once constants are updated one needs to re-build the eggfile 
  - Server will run forever till the time it is terminated or window is closed   
  - sqllite db (which comes with python) was used just to keep the counter of the producer messages which is used later for producer consumer reconciliation
  - The struct schema has been constructed based on the input JSON format
  - The measures have been calculated in the batch consumer and results are displayed on the console which can be stored in file format also based on the needs
  - Unit and integration tests were not added due to unavailability of time
  - There are shellscripts and batscripts folders, with their specific README.md
  - Kafka version used is specific to this project