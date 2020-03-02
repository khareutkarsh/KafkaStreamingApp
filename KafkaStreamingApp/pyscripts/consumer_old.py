from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from kafka import KafkaConsumer
import json

from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

KAFKA_CONSUMER_GROUP_NAME_CONS = "test_consumer_group"
KAFKA_TOPIC_NAME_CONS = "test"
KAFKA_OUTPUT_TOPIC_NAME_CONS="test_output"

KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .master("local[*]") \
        .config("spark.jars", "file:///F://kafka_2.12-2.4.0//libs//spark-sql-kafka-0-10_2.11-2.4.0.jar,file:///F://kafka_2.12-2.4.0//libs//kafka-clients-2.4.0.jar") \
        .config("spark.executor.extraClassPath", "file:///F://kafka_2.12-2.4.0//libs//spark-sql-kafka-0-10_2.11-2.4.0.jar,file:///F://kafka_2.12-2.4.0//libs//kafka-clients-2.4.0.jar") \
        .config("spark.executor.extraLibrary", "file:///F://kafka_2.12-2.4.0//libs//spark-sql-kafka-0-10_2.11-2.4.0.jar,file:///F://kafka_2.12-2.4.0//libs//kafka-clients-2.4.0.jar") \
        .config("spark.driver.extraClassPath", "file:///F://kafka_2.12-2.4.0//libs//spark-sql-kafka-0-10_2.11-2.4.0.jar,file:///F://kafka_2.12-2.4.0//libs//kafka-clients-2.4.0.jar") \
        .getOrCreate()
'''
F:\Hadoop
spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()'''
spark.sparkContext.setLogLevel("ERROR")

'''spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()'''
inputDf = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
  .option("subscribe", "test") \
  .option("startingOffsets", "latest") \
  .load()
inputDf.printSchema()
personJsonDF=inputDf.selectExpr("CAST(value AS STRING)","timestamp")
'''df_write_stream = personJsonDF \
    .writeStream \
    .trigger(processingTime='2 seconds') \
    .outputMode("append") \
    .format("console") \
    .start()'''
person_schema = StructType() \
    .add("id", IntegerType()) \
    .add("first_name", StringType()) \
    .add("last_name", StringType()) \
    .add("email", StringType())\
    .add("gender", StringType())\
    .add("ip_address",StringType())\
    .add("date",StringType())\
    .add("country",StringType())

person_df2 = personJsonDF.select(from_json("value", person_schema).alias("person_detail"),"timestamp")
person_df2.printSchema()
person_df3 = person_df2.select("person_detail.*","timestamp")
person_df3.printSchema()
person_df4 = person_df3.withColumn("key", F.lit(100))\
                                                    .withColumn("value", F.concat(F.lit("{'firstname': '"), \
                                                    F.col("first_name"), F.lit("', 'email': '"), \
                                                    F.col("email").cast("string"), F.lit("'}")))

'''df_write_stream = person_df3 \
    .writeStream \
    .trigger(processingTime='2 seconds') \
    .outputMode("append") \
    .format("console") \
    .start()'''



df_write_stream2 = person_df4 \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
    .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS) \
    .trigger(processingTime='2 seconds') \
    .outputMode("update") \
    .option("checkpointLocation", "file:///F://Hadoop//spark_structured_streaming_kafka//py_checkpoint") \
    .start()
#df.show()
'''
transaction_detail_df1 = df.selectExpr("CAST(value AS STRING) as json", "timestamp")
transaction_detail_df1.printSchema()

# Define a schema for the transaction_detail data
transaction_detail_schema = StructType() \
    .add("id", IntegerType()) \
    .add("firstname", StringType()) \
    .add("lastname", StringType()) \
    .add("email", StringType())\
    .add("gender", StringType())\
    .add("ipaddress",StringType())\
    .add("date",TimestampType())\
    .add("country",StringType())

transaction_detail_df2 = transaction_detail_df1 \
    .select(from_json("json", transaction_detail_schema).alias("transaction_detail"), "timestamp")

transaction_detail_df3 = transaction_detail_df2.select("transaction_detail.*", "timestamp")

#df2 = df1.selecr

transaction_detail_df3.printSchema()

#transaction_detail_df3.collect()

# Write final result into console for debugging purpose
trans_detail_write_stream = transaction_detail_df3 \
    .writeStream \
    .trigger(processingTime='2 seconds') \
    .outputMode("update") \
    .option("truncate", "false")\
    .format("console") \
    .start()


# Write key-value data from a DataFrame to a specific Kafka topic specified in an option
trans_detail_write_stream_1 = transaction_detail_df3 \
    .selectExpr("CAST(timestamp AS STRING)", "CAST(firstname AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
    .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS) \
    .trigger(processingTime='2 seconds') \
    .outputMode("update") \
    .option("checkpointLocation", "file:///F://Hadoop//spark_structured_streaming_kafka//py_checkpoint") \
    .start()
'''


'''trans_detail_write_stream = transaction_detail_df3 \
    .writeStream \
    .format("json") \
    .option("path", "file:///F://Hadoop//spark_structured_streaming_kafka//streamdata.txt") \
    .start()
trans_detail_write_stream = transaction_detail_df3 \
 .writeStream \
 .format("csv") \
 .option("format", "append") \
 .option("checkpointLocation", "file:///F://Hadoop//spark_structured_streaming_kafka//py_checkpoint") \
 .option("path", "file:///F://Hadoop//streamdata.txt") \
 .outputMode("append") \
 .start()'''

df_write_stream2.awaitTermination()

print("PySpark Structured Streaming with Kafka Demo Application Completed.")
#df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
'''
consumer = KafkaConsumer(
    KAFKA_TOPIC_NAME_CONS,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=KAFKA_CONSUMER_GROUP_NAME_CONS,
    value_deserializer=lambda x: x.decode())

for message in consumer:
    print(message)

'''