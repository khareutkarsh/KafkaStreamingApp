#!/bin/bash
. ../config/script_conf

cd ../dist
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,org.apache.kafka:kafka-clients:2.4.0 --py-files ${EGG_FILE_NAME} ../__main__.py STREAM_CONSUMER ${STREAM_CONSUMER_NAME} ${KAFKA_BOOTSTRAP_SERVERS} ${KAFKA_INPUT_TOPIC_NAME} ${KAFKA_OUTPUT_TOPIC_NAME} ${CHECKPOINT_FILE_DIR} ${KAFKA_WINDOW_TIME} ${OUTPUT_FILE_DIR}
