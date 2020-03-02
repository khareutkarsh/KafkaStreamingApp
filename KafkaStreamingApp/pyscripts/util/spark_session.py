"""
This is the file to create spark session
"""
from pyspark.sql import SparkSession

# get spark session if jars are added as dependency during spark submit
def get_spark_session(logger):
    try:
        spark = SparkSession \
            .builder \
            .appName("PySpark Structured Streaming with Kafka") \
            .enableHiveSupport() \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
    except Exception as e:
        logger.error(e)
    return spark

# get spark session if jars are not added as dependency during spark submit
def get_spark_session_with_jars(logger):
    try:
        spark = SparkSession \
            .builder \
            .appName("PySpark Structured Streaming with Kafka") \
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
    except Exception as e:
        logger.error(e)
    return spark
