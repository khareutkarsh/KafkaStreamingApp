"""
This is a main file to execute respective app modules
"""
import sys

from pyscripts.consumer.kafka_batch_consumer import KafkaBatchConsumer
from pyscripts.consumer.kafka_stream_consumer import KafkaStreamConsumer
from pyscripts.monitoring.prod_cons_recon import ProdConsRecon
from pyscripts.producer.kafka_stream_producer import KafkaStreamProducer

if __name__ == "__main__":

    if sys.argv[1] == "BATCH_CONSUMER":
        if sys.argv[2] and sys.argv[3] and sys.argv[4] and sys.argv[5] and sys.argv[6]:
            consumerObj=KafkaBatchConsumer(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
            consumerObj.process_measures_from_grouped_message()

    elif sys.argv[1] == "STREAM_CONSUMER":
        if sys.argv[2] and sys.argv[3] and sys.argv[4] and sys.argv[5] and sys.argv[6] and sys.argv[7] and sys.argv[8]:
            consumerObj=KafkaStreamConsumer(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6],sys.argv[7], sys.argv[8])
            consumerObj.stream_and_process_message()

    if sys.argv[1] == "STREAM_PRODUCER":
        if sys.argv[2] and sys.argv[3] and sys.argv[4] and sys.argv[5] and sys.argv[6] and sys.argv[7]:
            producerObj=KafkaStreamProducer(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7])
            kafka_producer=producerObj.get_kafka_producer()
            producerObj.put_message_on_kafka(kafka_producer)

    elif sys.argv[1] == "PROD_CONS_RECON":
        if sys.argv[2] and sys.argv[3] and sys.argv[4]:
            reconObj = ProdConsRecon(sys.argv[2], sys.argv[3], sys.argv[4])
            reconObj.producer_consumer_recon()
