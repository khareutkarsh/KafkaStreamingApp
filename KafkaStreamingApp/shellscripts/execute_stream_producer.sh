#!/bin/bash
. ../config/script_conf
python ../pyscripts/producer/kafka_stream_producer_main.py PRODUCER_NAME KAFKA_BOOTSTRAP_SERVERS KAFKA_INPUT_TOPIC_NAME INPUT_JSON_FILE KAFKA_PRODUCER_SLEEP_TIME
