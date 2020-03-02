set EGG_FILE_NAME=KafkaStreamingApp-1.0.0-py3.7.egg
set KAFKA_INPUT_TOPIC_NAME="test_input"
set KAFKA_OUTPUT_TOPIC_NAME="test_staging"
set KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:9092"
set TIME_WINDOW="5 minutes"
set CHECKPOINT_FILE_DIR="file:///F://Hadoop//spark_structured_streaming_kafka//"
set STREAM_CONSUMER_NAME="py_stream_consumer"
set KAFKA_WINDOW_TIME="5 minutes"
set OUTPUT_FILE_DIR=F:\python\QA\stkf\KafkaStreamingApp\resources/output/data
cd ../dist


spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,org.apache.kafka:kafka-clients:2.4.0 --py-files %EGG_FILE_NAME% ../__main__.py STREAM_CONSUMER %STREAM_CONSUMER_NAME% %KAFKA_BOOTSTRAP_SERVERS% %KAFKA_INPUT_TOPIC_NAME% %KAFKA_OUTPUT_TOPIC_NAME% %CHECKPOINT_FILE_DIR% %KAFKA_WINDOW_TIME% %OUTPUT_FILE_DIR%
