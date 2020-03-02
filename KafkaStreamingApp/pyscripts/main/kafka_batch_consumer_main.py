"""
This is a main file to execute respective app modules
"""
import sys

from pyscripts.consumer.kafka_batch_consumer import KafkaBatchConsumer

if __name__ == "__main__":
    if sys.argv[1] and sys.argv[2] and sys.argv[3]:
        consumerObj=KafkaBatchConsumer(sys.argv[1], sys.argv[2], sys.argv[3])
        consumerObj.process_measures_from_grouped_message()