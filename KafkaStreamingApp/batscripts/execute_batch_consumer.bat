set EGG_FILE_NAME=KafkaStreamingApp-1.0.0-py3.7.egg
set KAFKA_OUTPUT_TOPIC_NAME="test_staging"
set KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:9092"
set BATCH_CONSUMER_NAME="py_batch_consumer"
set KAFKA_WINDOW_TIME="5 minutes"
set OUTPUT_FILE_DIR=F:\python\QA\stkf\KafkaStreamingApp\resources/output/measures
cd ../dist

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,org.apache.kafka:kafka-clients:2.4.0 --py-files %EGG_FILE_NAME% ../__main__.py BATCH_CONSUMER %BATCH_CONSUMER_NAME% %KAFKA_BOOTSTRAP_SERVERS% %KAFKA_OUTPUT_TOPIC_NAME% %KAFKA_WINDOW_TIME% %OUTPUT_FILE_DIR%
