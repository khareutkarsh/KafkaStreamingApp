"""
This is a main file to execute respective app modules
"""
from pyscripts.consumer.kafka_batch_consumer import KafkaBatchConsumer

if __name__ == "__main__":
    consumerObj=KafkaBatchConsumer()
    consumerObj.process_measures_from_grouped_message()