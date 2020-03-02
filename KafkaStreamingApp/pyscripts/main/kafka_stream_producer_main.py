"""
This is a main file to execute respective app modules
"""
from pyscripts.producer.kafka_stream_producer import KafkaStreamProducer

if __name__ == "__main__":
    producerObj=KafkaStreamProducer("py_producer")
    kafka_producer=producerObj.get_kafka_producer()
    producerObj.put_message_on_kafka(kafka_producer)