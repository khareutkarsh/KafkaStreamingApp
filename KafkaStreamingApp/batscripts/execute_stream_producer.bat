set EGG_FILE_NAME=KafkaStreamingApp-1.0.0-py3.7.egg
set KAFKA_CONSUMER_GROUP_NAME = "consumer_group"
set KAFKA_INPUT_TOPIC_NAME = "test_input"
set KAFKA_OUTPUT_TOPIC_NAME = "test_staging"
set KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092","127.0.0.1:9093","127.0.0.1:9094"
set TIME_WINDOW="5 minutes"
set CHECKPOINT_FILE_DIR="file:///F://Hadoop//spark_structured_streaming_kafka//"
set KAFKA_HOME="F:\kafka_2.12-2.4.0"
set INPUT_JSON_FILE="MOCK_DATA.json"
set KAFKA_PRODUCER_SLEEP_TIME=2
set PRODUCER_NAME="py_producer"
set STREAM_CONSUMER_NAME="py_stream_consumer"
set BATCH_CONSUMER_NAME="py_batch_consumer"
set MBEAN="kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec"
python ../pyscripts/producer/kafka_stream_producer_main.py PRODUCER_NAME KAFKA_BOOTSTRAP_SERVERS KAFKA_INPUT_TOPIC_NAME INPUT_JSON_FILE KAFKA_PRODUCER_SLEEP_TIME
