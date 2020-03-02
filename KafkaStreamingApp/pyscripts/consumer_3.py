from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from kafka import KafkaConsumer
from pyscripts.constants.app_constants import *
from pyspark.sql.functions import window
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

max_count = 0
min_count = 0
spark = SparkSession \
    .builder \
    .appName("PySpark Structured Streaming with Kafka Demo") \
    .master("local[*]") \
    .config("spark.jars",
            "file:///F://kafka_2.12-2.4.0//libs//spark-sql-kafka-0-10_2.11-2.4.0.jar,file:///F://kafka_2.12-2.4.0//libs//kafka-clients-2.4.0.jar") \
    .config("spark.executor.extraClassPath",
            "file:///F://kafka_2.12-2.4.0//libs//spark-sql-kafka-0-10_2.11-2.4.0.jar,file:///F://kafka_2.12-2.4.0//libs//kafka-clients-2.4.0.jar") \
    .config("spark.executor.extraLibrary",
            "file:///F://kafka_2.12-2.4.0//libs//spark-sql-kafka-0-10_2.11-2.4.0.jar,file:///F://kafka_2.12-2.4.0//libs//kafka-clients-2.4.0.jar") \
    .config("spark.driver.extraClassPath",
            "file:///F://kafka_2.12-2.4.0//libs//spark-sql-kafka-0-10_2.11-2.4.0.jar,file:///F://kafka_2.12-2.4.0//libs//kafka-clients-2.4.0.jar") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

inputDf = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
    .option("subscribe", "test") \
    .option("startingOffsets", "earliest") \
    .load()
inputDf.printSchema()

personJsonDF = inputDf.selectExpr("CAST(value AS STRING)", "timestamp")

person_schema = StructType() \
    .add("id", IntegerType()) \
    .add("first_name", StringType()) \
    .add("last_name", StringType()) \
    .add("email", StringType()) \
    .add("gender", StringType()) \
    .add("ip_address", StringType()) \
    .add("date", StringType()) \
    .add("country", StringType())

person_df2 = personJsonDF.select(from_json("value", person_schema).alias("person_detail"), "timestamp")
person_df3 = person_df2.select("person_detail.*", "timestamp")


person_df4 = person_df3.select(
    F.col("id"), \
    F.when(F.col("first_name").isNull(), 'NULL').otherwise(F.col("first_name")).alias("first_name"), \
    F.col("last_name"), \
    F.when(F.col("email").isNull(), 'NULL').otherwise(F.col("email")).alias("email"), \
    F.col("gender"), \
    # F.when(F.col("ip_address").rlike(IP_REGEX),F.col("ip_address")).otherwise(DEFAULT_IP), \
    F.date_format(F.col("date"), 'dd/MM/yyyy').alias("date"), \
    F.initcap(F.col("country")).alias("country"), \
    F.col("timestamp") \
    )

country_count_df1 = person_df4.withWatermark("timestamp", "10 minutes").groupBy(
    window(person_df4.timestamp, "10 minutes", "5 minutes"), 'country').count().orderBy('window', 'count')

person_measures_df3 = person_df4.groupBy(window(person_df4.timestamp, "10 minutes", "5 minutes")).agg(
    F.approx_count_distinct("email").alias("unique_users")).orderBy('window', 'unique_users')

person_measures_df5 = person_measures_df3.withColumn("key", F.col('window')) \
    .withColumn("value", F.concat(F.lit("'{'time_window': '"), F.col('window').cast(StringType()),
                                  F.lit("', 'number of unique users': '"), \
                                  F.col('unique_users'), F.lit("'}")))

df_write_stream = person_measures_df5 \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
    .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS) \
    .trigger(processingTime='2 seconds') \
    .outputMode("complete") \
    .option("checkpointLocation", "file:///F://Hadoop//spark_structured_streaming_kafka//py_checkpoint2") \
    .start()

person_df4.writeStream \
    .format("parquet") \
    .option("startingOffsets", "earliest") \
    .option("path", "file:///C://Users//Utkarsh//PycharmProjects//KafkaStreamConsumer//resources//data") \
    .option("checkpointLocation", "file:///F://Hadoop//spark_structured_streaming_kafka//py_checkpoint3") \
    .start()

df_write_stream.awaitTermination()

print("PySpark Structured Streaming with Kafka Demo Application Completed.")
