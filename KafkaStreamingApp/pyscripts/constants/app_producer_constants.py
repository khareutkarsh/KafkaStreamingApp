"""
This is the constants file where all the producer app constants can be configured
"""
import os

BROKERS = "127.0.0.1:9092","127.0.0.1:9093","127.0.0.1:9094"
TOPIC= "test_input"

INPUT_JSON_FILE="MOCK_DATA.json"
SLEEP_TIME=2

dir_name = os.path.dirname(__file__)
DB_NAME="status.db"
DB_FILE_PATH='/'.join((dir_name, '../../database',DB_NAME ))

