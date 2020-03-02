"""
This is the constants file where all the producer app constants can be configured
"""
import os

dir_name = os.path.dirname(__file__)
DB_NAME="status.db"
DB_FILE_PATH='/'.join((dir_name, '../../database',DB_NAME ))

