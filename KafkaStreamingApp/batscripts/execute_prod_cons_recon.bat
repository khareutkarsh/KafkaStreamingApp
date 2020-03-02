set EGG_FILE_NAME=KafkaStreamingApp-1.0.0-py3.7.egg
set PRODUCER_NAME="py_producer"
set DB_FILE_PATH=F:\python\QA\stkf\KafkaStreamingApp\database/status.db
set OUTPUT_FILE_DIR=F:\python\QA\stkf\KafkaStreamingApp\resources/output/data
cd ../dist
python %EGG_FILE_NAME% PROD_CONS_RECON %PRODUCER_NAME% %DB_FILE_PATH% %OUTPUT_FILE_DIR%
