"""This is the class which reads the stream from Kafka topic in a streaming fashion and sends the grouped data on to
a new topic. This class also saves the cleansed data into parquet format """

import os
from pyspark.sql.types import StringType
from pyscripts.util.spark_session import get_spark_session
import pyspark.sql.functions as F
from pyscripts.constants.app_consumer_constants import *
from pyspark.sql.functions import window
from pyspark.sql.functions import from_json
from pyscripts.util.logging_util import get_logger
from pyscripts.util.message_schema import get_message_schema


class KafkaStreamConsumer:

    # Method to initialize the class
    def __init__(self,consumer_name,kafka_bootstrap_servers,kafka_input_topic_name,kafka_output_topic_name,checkpoint_file_dir,kafka_window_time,output_file_dir):
        self.logger = get_logger()
        self.spark = get_spark_session(self.logger)
        self.message_schema = get_message_schema()
        self.dir_name = os.path.dirname(__file__)
        self.consumer_name=consumer_name
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_input_topic_name = kafka_input_topic_name
        self.kafka_output_topic_name = kafka_output_topic_name
        self.checkpoint_file_dir=checkpoint_file_dir
        self.kafka_window_time=kafka_window_time
        self.output_file_dir=output_file_dir

        # Method to read the kafka stream
    def get_kafka_consumer(self):
        input_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_input_topic_name) \
            .option("auto.offset.reset", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING)", "timestamp")
        return input_df

    # Method to process the stream for data cleansing and publishing grouped data
    def stream_and_process_message(self):
        try:
            input_df = self.get_kafka_consumer()
            flat_message_df = self.get_parsed_message(input_df)
            cleaned_message_df = self.get_cleaned_message(flat_message_df)
            grouped_message_df = cleaned_message_df.select('id', window(cleaned_message_df.timestamp, self.kafka_window_time),
                                                           'country',
                                                           'email')
            grouped_message_output_df = self.get_output_df(grouped_message_df)

            # publish the grouped data onto new topic for further measure calculation
            df_write_stream = grouped_message_output_df \
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                .writeStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("topic", self.kafka_output_topic_name) \
                .outputMode("append") \
                .option("checkpointLocation", self.checkpoint_file_dir + "output_topic_checkpoint") \
                .start()

            # save the cleansed data in parquet file format for future use
            cleaned_message_df \
                .writeStream \
                .format("parquet") \
                .option("startingOffsets", "earliest") \
                .option("path", self.output_file_dir ) \
                .option("checkpointLocation", self.checkpoint_file_dir + "saved_parquet_checkpoint") \
                .start()

            df_write_stream.awaitTermination()
        except Exception as e:
            self.logger.error(e)

    # Method to parse the incoming message
    def get_parsed_message(self, input_df):
        message_df = input_df.select(from_json("value", self.message_schema).alias("message_detail"), "timestamp")
        flat_message_df = message_df.select("message_detail.*", "timestamp")
        return flat_message_df

    # Method to clean the data
    def get_cleaned_message(self, flat_message_df):
        cleaned_message_df = flat_message_df.select(
            F.col("id"), \
            F.when(F.col("first_name").isNull(), 'NULL').otherwise(F.col("first_name")).alias("first_name"), \
            F.col("last_name"), \
            F.when(F.col("email").isNull(), 'NULL').otherwise(F.col("email")).alias("email"), \
            F.col("gender"), \
            F.col("ip_address"),\
            F.date_format(F.col("date"), 'dd/MM/yyyy').alias("date"), \
            F.initcap(F.col("country")).alias("country"), \
            F.col("timestamp") \
            ).withColumn("ip_addr",F.when(F.col("ip_address").rlike(IP_REGEX),F.col("ip_address")).otherwise(DEFAULT_IP)).drop(F.col("ip_address"))
        return cleaned_message_df

    # Method to add key value columns to publish the grouped data on new topic
    def get_output_df(self, grouped_message_df):
        grouped_message_output_df = grouped_message_df.withColumn("key", F.col("window")) \
            .withColumn("value", F.concat(F.lit("{'time_window': '"), \
                                          F.col('window').cast(StringType()), F.lit("', 'country': '"), \
                                          F.col('country'), F.lit("', 'users': '"), \
                                          F.col('email').cast(StringType()), F.lit("'}")))
        return grouped_message_output_df
