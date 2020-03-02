# Bat scripts for automation 

These scripts are written to provide automated building of the project and running test cases

##Execution

- Build the egg file by executing following script
```shell script
$KafkaStreamingApp/shellscirpts>build_egg.bat
$KafkaStreamingApp/shellscirpts>execute_stream_producer.bat
$KafkaStreamingApp/shellscirpts>execute_stream_consumer.bat
$KafkaStreamingApp/shellscirpts>execute_batch_consumer.bat
$KafkaStreamingApp/shellscirpts>execute_prod_cons_recon.bat
$KafkaStreamingApp/shellscirpts>execute_kafka_jmx_monitoring.bat
$KafkaStreamingApp/shellscirpts>execute_kafka_runclass.bat
```
##Script Details
#### build_egg.bat
This file is used to build the egg file of the project
### execute_stream_producer.bat
This file is used to run the producer application (Given the kafka brokers and topics are running)
### execute_stream_consumer.bat
This file is used to run the consumer application which groups the stream data and sends it to another topic(Given the kafka brokers and topics are running)
### execute_batch_consumer.bat
This file is used to run the consumer application that reads the grouped data and calculate measures (Given the kafka brokers and topics are running)
### execute_prod_cons_recon.bat
This file is used to run the reconciliation application that will verify in between whether the number of messages sent by producer are equal to number of messages processed by the consumer (Given the kafka brokers and topics are running)
### execute_kafka_jmx_monitoring.bat
This file is used to run the monitoring tool jmx for kafka for a particular configuration. For better visualizations one should open JAVA_HOME/bin/jconsole  (Given the kafka brokers and topics are running)
### execute_kafka_runclass.bat
This file is used to run the MBEAN provided by python file  (Given the kafka brokers and topics are running)

