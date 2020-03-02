"""
This class is used to reconcile the counts of message sent by producer and received by consumer
"""
import os
from pyscripts.constants.app_producer_constants import DB_FILE_PATH
from pyscripts.util.db_utils import get_db_connection, get_processstatuslog_db
from pyscripts.util.logging_util import get_logger
from pyscripts.util.spark_session import get_spark_session


class ProdConsRecon:

    # Method to initialize the class variables
    def __init__(self, producer_name):
        self.logger = get_logger()
        self.producer_name = producer_name
        self.spark = get_spark_session()
        self.dir_name = os.path.dirname(__file__)

    # Method to reconcile the count between producer and consumer
    def producer_consumer_recon(self):
        conn = get_db_connection(DB_FILE_PATH, self.logger)
        cursor = conn.cursor()
        try:
            consumer_message_count = self.spark.read.parquet(self.dir_name + "/../../resources/output/data").count()
            rows = get_processstatuslog_db(cursor, "producer_message_log", self.producer_name, self.logger)
            for row in rows:
                producer_message_count = row[2]

            if consumer_message_count == int(producer_message_count):
                self.logger.info(
                    "number of messages processed by the consumer is equal to the number of messages pushed by the "
                    "producer")
            elif consumer_message_count < int(producer_message_count):
                self.logger.info("the consumer is lagging behind the producer by:" + int(
                    producer_message_count) - consumer_message_count + " messages")
        except Exception as e:
            self.logger.error(e)
            return []
        finally:
            conn.close()
