First we make a config file for each of the brokers (on Windows use the copy command instead):

> cp config/server.properties config/server-1.properties
> cp config/server.properties config/server-2.properties
Now edit these new files and set the following properties:


config/server-1.properties:
    broker.id=1
    listeners=PLAINTEXT://:9093
    log.dirs=/tmp/kafka-logs-1
 
config/server-2.properties:
    broker.id=2
    listeners=PLAINTEXT://:9094
    log.dirs=/tmp/kafka-logs-2
The broker.id property is the unique and permanent name of each node in the cluster. We have to override the port and log directory only because we are running these all on the same machine and we want to keep the brokers from all trying to register on the same port or overwrite each other's data.

For JMX tool edit the bin/kafka-server-start.sh

export JMX_PORT=9999


Note: This package already has 3 broker configurations and JMX_PORT set to 9999. Just run the below commands to start the kafka broker and topics


Run the following commands

>bin/zookeeper-server-start.sh config/zookeeper.properties

>bin/kafka-server-start.sh config/server.properties


We already have Zookeeper and our single node started, so we just need to start the two new nodes:


> bin/kafka-server-start.sh config/server-1.properties &
...
> bin/kafka-server-start.sh config/server-2.properties &
...

>bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 3 --partitions 1 --topic test_input

>bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 3 --partitions 1 --topic test_staging