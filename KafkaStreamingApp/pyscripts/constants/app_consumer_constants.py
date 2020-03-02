"""
This is the constants file where all the producer app constants can be configured
"""
import os

KAFKA_CONSUMER_GROUP_NAME = "consumer_group"
KAFKA_INPUT_TOPIC_NAME = "test_input"
KAFKA_OUTPUT_TOPIC_NAME = "test_staging"
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092","127.0.0.1:9093","127.0.0.1:9094"
TIME_WINDOW="5 minutes"
CHECKPOINT_FILE_DIR="file:///F://Hadoop//spark_structured_streaming_kafka//"

KAFKA_HOME="F:\kafka_2.12-2.4.0"

DEFAULT_IP="127.127.127.127"
IP_REGEX="^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$"

dir_name = os.path.dirname(__file__)
JMX_SCRIPT_PATH=dir_name+"/../batscripts/execute_kafka_runclass.bat"
MBEAN="kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec"