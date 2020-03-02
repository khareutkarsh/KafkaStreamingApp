"""
This is the file where the grouped data is read from the second topic and measures are calculated
"""
import os
import sys

import pyspark.sql.functions as F
from pyscripts.constants.app_consumer_constants import *
from pyspark.sql.window import Window
from pyspark.sql.functions import from_json
from pyscripts.util.logging_util import get_logger
from pyscripts.util.message_schema import get_grouped_message_schema
from pyscripts.util.spark_session import get_spark_session


class KafkaBatchConsumer:

    # Method for initializing the class
    def __init__(self,consumer_name,kafka_bootstrap_servers,kafka_output_topic_name,kafka_window_time,output_file_dir):
        self.logger = get_logger()
        self.spark = get_spark_session(self.logger)
        self.grouped_schema = get_grouped_message_schema()
        self.consumer_name = consumer_name
        self.dir_name = os.path.dirname(__file__)
        self.kafka_bootstrap_servers=kafka_bootstrap_servers
        self.kafka_output_topic_name=kafka_output_topic_name
        self.kafka_window_time = kafka_window_time
        self.output_file_dir = output_file_dir

    # Method to read the kafka topic
    def get_kafka_consumer(self):
        inputDf = self.spark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_output_topic_name) \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING)", "timestamp")
        return inputDf

    # Method to process the measures
    def process_measures_from_grouped_message(self):
        try:
            input_df = self.get_kafka_consumer()
            flat_message_df = self.get_parsed_message(input_df)

            unique_users_measure_df = flat_message_df.groupBy('time_window').agg(
                F.approx_count_distinct("users").alias("unique_users")).orderBy('time_window', 'unique_users')

            country_count_df1 = flat_message_df.groupBy('time_window', 'country').count().orderBy('time_window', 'count')
            most_represented_countries_df = country_count_df1.withColumn("rank_max", F.rank().over(
                Window.partitionBy("time_window").orderBy(F.desc("count")))).where(F.col("rank_max") == 1).orderBy(
                "time_window").select(F.col("time_window"), F.col("country").alias("most_represented_country"), F.col("count").alias("most_represented_country_count"))
            least_represented_countries_df = country_count_df1.withColumn("rank_min", F.rank().over(
                Window.partitionBy("time_window").orderBy(F.col("count")))).where(F.col("rank_min") == 1).orderBy(
                "time_window").select(F.col("time_window"), F.col("country").alias("least_represented_country"), F.col("count").alias("least_represented_country_count"))

            most_represented_country_df  = self.get_first_record_from_grouped_data(most_represented_countries_df,"most_represented_country")
            least_represented_country_df = self.get_first_record_from_grouped_data(least_represented_countries_df,"least_represented_country")

            measures_df = most_represented_country_df.join(least_represented_country_df,"time_window").join(unique_users_measure_df,"time_window")\
                .select(most_represented_countries_df.time_window,F.col("most_represented_country")\
                        ,F.col("most_represented_country_count"),F.col("least_represented_country")\
                        ,F.col("least_represented_country_count"),F.col("unique_users"))


            measures_df.write.format("csv").save(self.output_file_dir)

            #self.logger.info(most_represented_country_df.collect())
            #self.logger.info(least_represented_country_df.collect())
            #self.logger.info(unique_users_measure_df.collect())
        except Exception as e:
            self.logger.error(e)

    # Method to parse the incoming message
    def get_parsed_message(self, input_df):
        message_df = input_df.select(from_json("value", self.grouped_schema).alias("grouped_data"), "timestamp")
        flat_message_df = message_df.select("grouped_data.*", "timestamp")
        return flat_message_df

    def get_first_record_from_grouped_data(self,grouped_df,order_by_col):
        wndw = Window.partitionBy("time_window").orderBy(F.col(order_by_col))
        limited_records_df = grouped_df.withColumn("row_num",F.row_number().over(wndw)).where(F.col("row_num")== 1).drop(F.col("row_num"))
        return limited_records_df

