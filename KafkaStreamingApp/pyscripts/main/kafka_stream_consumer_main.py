"""
This is a main file to execute respective app modules
"""
from pyscripts.consumer.kafka_stream_consumer import KafkaStreamConsumer

if __name__ == "__main__":
    consumerObj=KafkaStreamConsumer("py_stream_consumer")
    consumerObj.stream_and_process_message()