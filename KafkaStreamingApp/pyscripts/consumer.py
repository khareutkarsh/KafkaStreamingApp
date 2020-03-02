from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from kafka import KafkaConsumer
import json
from pyspark.sql.functions import window
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

KAFKA_CONSUMER_GROUP_NAME_CONS = "test_consumer_group"
KAFKA_TOPIC_NAME_CONS = "test"
KAFKA_OUTPUT_TOPIC_NAME_CONS="test_output"

KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'
max_count=0
min_count=0
spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .master("local[*]") \
        .config("spark.jars", "file:///F://kafka_2.12-2.4.0//libs//spark-sql-kafka-0-10_2.11-2.4.0.jar,file:///F://kafka_2.12-2.4.0//libs//kafka-clients-2.4.0.jar") \
        .config("spark.executor.extraClassPath", "file:///F://kafka_2.12-2.4.0//libs//spark-sql-kafka-0-10_2.11-2.4.0.jar,file:///F://kafka_2.12-2.4.0//libs//kafka-clients-2.4.0.jar") \
        .config("spark.executor.extraLibrary", "file:///F://kafka_2.12-2.4.0//libs//spark-sql-kafka-0-10_2.11-2.4.0.jar,file:///F://kafka_2.12-2.4.0//libs//kafka-clients-2.4.0.jar") \
        .config("spark.driver.extraClassPath", "file:///F://kafka_2.12-2.4.0//libs//spark-sql-kafka-0-10_2.11-2.4.0.jar,file:///F://kafka_2.12-2.4.0//libs//kafka-clients-2.4.0.jar") \
        .enableHiveSupport()\
        .getOrCreate()

#spark.sparkContext.setLogLevel("ERROR")


inputDf = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
  .option("subscribe", "test") \
  .option("startingOffsets", "earliest") \
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

person_df4=person_df3.select(
    F.col("id"), \
    F.when(F.col("first_name").isNull(), 'NULL').otherwise(F.col("first_name")).alias("first_name"),\
    F.col("last_name"),\
    F.when(F.col("email").isNull(), 'NULL').otherwise(F.col("email")).alias("email"),\
    F.col("gender"),\
    F.col("ip_address"),\
    F.date_format(F.col("date"), 'dd/MM/yyyy').alias("date"),\
    F.initcap(F.col("country")).alias("country"),\
    F.col("timestamp")\
    )

#grouped = person_df4.groupBy(window(person_df4.timestamp, "2 minutes"),person_df4.country).count().agg(F.collect_list("window").alias("window"))
#sorted = grouped.withColumn("dates", F.sort_array("window", asc=False))
#most_recent = sorted.selectExpr("window", "dates[0]")

#person_df4.createOrReplaceTempView("df_1")
country_count_df1=person_df4.groupBy(window(person_df4.timestamp, "2 minutes"),'country').count().orderBy('window','count')
#country_count_df2=country_count_df1.groupBy('window').count()
#df2=spark.sql("select max(country),min(country) from df_1")

#country_count_df1.createOrReplaceTempView("country_count_window")
#c2_df = spark.sql("select window,country ,count from country_count_window t1 where NOT exists (select count from country_count_window t2 where t2.count>t1.count)")
#country_count_df2=country_count_df1.agg(F.max(F.col("count")))
#country_count_df1=person_df4.groupby('country').agg(F.count('country').alias('country_count')).where(F.col('country').isNotNull())
#df_rank_max=country_count_df1.withColumn("rank_max",F.rank().over(window.partitionBy().orderBy(F.desc("country_count")))).where(F.col("rank_max")==1)


#person_measures_df2=person_measures_df1.groupBy("country").agg(F.max("country_count"))
person_measures_df3=person_df4.groupBy(window(person_df4.timestamp, "2 minutes")).agg(F.approx_count_distinct("email").alias("unique_users")).orderBy('window','unique_users')

#result_df1 = country_count_df1.join(person_measures_df3, 'window')
'''person_df5 = result_df1.withColumn("key", F.col("window"))\
                                                    .withColumn("value", F.concat(F.lit("{'most represented country': '"), \
                                                    result_df1.country, F.lit("', 'least represented country': '"), \
                                                    result_df1.country, F.lit("', 'number of unique users': '"), \
                                                    result_df1.unique_users,F.lit("'}")))'''


def processRow(row):
    type(row)
    '''if row.count>max_count:
        max_count=row.count
        max_country=row.country
    if row.count<min_count:
        min_count=row.count
        min_country=row.country'''

def foreach_batch_function(df, epoch_id):
    try:
        #df.show(25)
        print("**************************************************************************************")

        #df.write.mode("overwrite").saveAsTable("test_db.test_table2")
        path = 'file:///C://Users//Utkarsh//PycharmProjects//KafkaStreamConsumer//pyscripts'
        #df.createOrReplaceTempView("df_table")
        #df2=spark.sql("select window,max(country_count),min(country_count) from df_table group by window")
        person_df4 = df.withColumn("key", F.lit(100)) \
            .withColumn("value", F.concat(F.lit("{'firstname': '"), \
                                          F.col("first_name"), F.lit("', 'email': '"), \
                                          F.col("email").cast("string"), F.lit("'}")))
        ds = person_df4 \
            .selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
            .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS)\
            .start()

    except:
        pass
    #df2=df.groupBy('window','country').agg(F.max(F.col("count"),F.min(F.col("count"))))
    #return df2

df_write_stream = person_df3 \
    .writeStream \
    .trigger(processingTime='2 minutes') \
    .outputMode("append") \
    .foreachBatch(foreach_batch_function)\
    .start()

'''df_write_stream2 = person_df5 \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
    .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS) \
    .trigger(processingTime='2 seconds') \
    .outputMode("update") \
    .option("checkpointLocation", "file:///F://Hadoop//spark_structured_streaming_kafka//py_checkpoint") \
    .start()'''
'''
def foreach_batch_function(person_df4):
    person_measures_df1 = person_df4.select("country").groupBy("country").agg(
        F.count("country").alias("country_count")).groupBy("country").agg(F.max("country_count"))
    return person_measures_df1


df_write_stream = person_df4 \
    .writeStream \
    .trigger(processingTime='2 seconds') \
    .outputMode("append") \
    .foreachBatch(foreach_batch_function(person_df4))\
    .format("console") \
    .start()'''
'''
person_df4 = person_df3.withColumn("key", F.lit(100))\
                                                    .withColumn("value", F.concat(F.lit("{'firstname': '"), \
                                                    F.col("first_name"), F.lit("', 'email': '"), \
                                                    F.col("email").cast("string"), F.lit("'}")))





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
'''
df_write_stream.awaitTermination()

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

'''def foreach_batch_function(person_df4):
    person_measures_df1 = person_df4.select("country").groupBy("country").agg(
        F.count("country").alias("country_count")).groupBy("country").agg(F.max("country_count"))
    return person_measures_df1'''

