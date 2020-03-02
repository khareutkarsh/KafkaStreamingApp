set EGG_FILE_NAME=KafkaStreamingApp-1.0.0-py3.7.egg
set KAFKA_INPUT_TOPIC_NAME="test_input"
set KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:9092"
set INPUT_JSON_FILE="F:\python\QA\stkf\KafkaStreamingApp\resources\input\MOCK_DATA.json"
set KAFKA_PRODUCER_SLEEP_TIME=2
set PRODUCER_NAME="py_producer"
set DB_FILE_PATH=F:\python\QA\stkf\KafkaStreamingApp\database/status.db
cd ../dist
python %EGG_FILE_NAME% STREAM_PRODUCER %PRODUCER_NAME% %KAFKA_BOOTSTRAP_SERVERS% %KAFKA_INPUT_TOPIC_NAME% %INPUT_JSON_FILE% %KAFKA_PRODUCER_SLEEP_TIME% %DB_FILE_PATH%
