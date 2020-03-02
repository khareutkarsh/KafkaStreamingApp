"""
This is a main file to execute respective app modules
"""
import sys

from pyscripts.producer.kafka_stream_producer import KafkaStreamProducer

if __name__ == "__main__":
    if sys.argv[1] and sys.argv[2] and sys.argv[3] and sys.argv[4] and sys.argv[5]:
        producerObj=KafkaStreamProducer(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
        kafka_producer=producerObj.get_kafka_producer()
        producerObj.put_message_on_kafka(kafka_producer)