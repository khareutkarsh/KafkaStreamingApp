#!/bin/bash
. ../config/script_conf
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,org.apache.kafka:kafka-clients:2.4.0 \
--py-files ../dist/${EGG_FILE_NAME} \
../pyscripts/consumer/kafka_stream_consumer_main.py STREAM_CONSUMER_NAME KAFKA_BOOTSTRAP_SERVERS KAFKA_INPUT_TOPIC_NAME KAFKA_OUTPUT_TOPIC_NAME CHECKPOINT_FILE_DIR
