"""
This is a main file to execute respective app modules
"""
import sys

from pyscripts.consumer.kafka_stream_consumer import KafkaStreamConsumer

if __name__ == "__main__":
    if sys.argv[1] and sys.argv[2] and sys.argv[3] and sys.argv[4] and sys.argv[5]:
        consumerObj=KafkaStreamConsumer(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
        consumerObj.stream_and_process_message()