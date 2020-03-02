"""
This class used to create a producer which reads the input JSON file and posts those records on the kafka topic
"""
from time import sleep
from kafka import KafkaProducer
import os
from pyscripts.constants.app_producer_constants import *
from pyscripts.util.db_utils import get_db_connection, initialize_db, insert_processstatuslog, update_processstatuslog
from pyscripts.util.logging_util import get_logger


class KafkaStreamProducer:

    # Method to initialize the class variables
    def __init__(self, producer_name,kafka_bootstrap_servers,kafka_input_topic_name,input_json_file,kafka_producer_sleep_time):
        logger = get_logger()
        self.producer_name = producer_name
        self.kafka_bootstrap_servers=kafka_bootstrap_servers
        self.kafka_input_topic_name=kafka_input_topic_name
        self.input_json_file=input_json_file
        self.kafka_producer_sleep_time=kafka_producer_sleep_time

    # initializing the DB and getting a connection
    def initialize_database(self):
        conn = get_db_connection(DB_FILE_PATH, self.logger)
        cursor = conn.cursor()
        initialize_db(cursor, self.logger)
        return conn, cursor

    # Method to get a connection to the kafka server
    def get_kafka_producer(self):
        kafka_producer = None
        try:
            kafka_producer = KafkaProducer(bootstrap_servers=[self.kafka_bootstrap_servers], value_serializer=lambda x: x.encode())
        except Exception as ex:
            self.logger.error('Exception while connecting Kafka')
            self.logger.error(str(ex))
        finally:
            return kafka_producer

    # Method to read the input file and post the messages on the kafka topic
    def put_message_on_kafka(self, kafka_producer):
        dir_name = os.path.dirname(__file__)
        test_file = dir_name + '/../../resources/' + self.input_json_file
        recordCount = 0
        # inserting one record in the status log and getting the recordid to return as a response
        conn, cursor = self.initialize_database()
        recordid = insert_processstatuslog(cursor, conn, "producer_message_log", self.producer_name, str(recordCount),
                                           self.logger)
        with open(test_file, 'r') as f:
            for line in f:
                record = line[:-1]
                kafka_producer.send(self.kafka_input_topic_name, value=record, )
                recordCount += 1
                update_processstatuslog(cursor, conn, recordid, "producer_message_log", str(recordCount))
                self.logger.info("Records written on topic :" + str(recordCount))
                sleep(self.kafka_producer_sleep_time)
