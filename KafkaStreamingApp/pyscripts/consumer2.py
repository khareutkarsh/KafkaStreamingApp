from pyspark.sql import SparkSession
from kafka import KafkaConsumer
import json

KAFKA_CONSUMER_GROUP_NAME_CONS = "test_consumer_group"
KAFKA_TOPIC_NAME_CONS = "test"

KAFKA_BOOTSTRAP_SERVERS_CONS = '127.0.0.1:9092'
'''
spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .master("local[*]") \
        .config("spark.jars", "file:///F://kafka_2.12-2.4.0//libs//spark-sql-kafka-0-10_2.12-2.4.0.jar,file:///F://kafka_2.12-2.4.0//libs//kafka-clients-2.4.0.jar") \
        .config("spark.executor.extraClassPath", "file:///F://kafka_2.12-2.4.0//libs//spark-sql-kafka-0-10_2.12-2.4.0.jar,file:///F://kafka_2.12-2.4.0//libs//kafka-clients-2.4.0.jar") \
        .config("spark.executor.extraLibrary", "file:///F://kafka_2.12-2.4.0//libs//spark-sql-kafka-0-10_2.12-2.4.0.jar,file:///F://kafka_2.12-2.4.0//libs//kafka-clients-2.4.0.jar") \
        .config("spark.driver.extraClassPath", "file:///F://kafka_2.12-2.4.0//libs//spark-sql-kafka-0-10_2.12-2.4.0.jar,file:///F://kafka_2.12-2.4.0//libs//kafka-clients-2.4.0.jar") \
        .getOrCreate()

F:\Hadoop
spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()'''
#spark.sparkContext.setLogLevel("ERROR")


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

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("StructuredNetworkWordCount") \
        .getOrCreate()
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("subscribe", "test") \
        .load()
    df.printSchema()