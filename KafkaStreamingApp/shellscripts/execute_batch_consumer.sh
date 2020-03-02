#!/bin/bash
. ../config/script_conf

cd ../dist
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,org.apache.kafka:kafka-clients:2.4.0 --py-files ${EGG_FILE_NAME} ../__main__.py BATCH_CONSUMER ${BATCH_CONSUMER_NAME} ${KAFKA_BOOTSTRAP_SERVERS} ${KAFKA_OUTPUT_TOPIC_NAME} ${KAFKA_WINDOW_TIME} ${OUTPUT_FILE_DIR}
