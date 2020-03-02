"""
This is a file to get message schema based on the structure of message
"""
from pyspark.sql.types import StructType, IntegerType, StringType


def get_message_schema():
    message_schema = StructType() \
        .add("id", IntegerType()) \
        .add("first_name", StringType()) \
        .add("last_name", StringType()) \
        .add("email", StringType()) \
        .add("gender", StringType()) \
        .add("ip_address", StringType()) \
        .add("date", StringType()) \
        .add("country", StringType())
    return message_schema


def get_grouped_message_schema():
    grouped_schema = StructType() \
        .add("time_window", StringType()) \
        .add("country", StringType()) \
        .add("users", StringType())
    return grouped_schema
