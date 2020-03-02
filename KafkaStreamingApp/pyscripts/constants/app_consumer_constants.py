"""
This is the constants file where all the producer app constants can be configured
"""
import os

DEFAULT_IP="127.127.127.127"
IP_REGEX="^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$"

dir_name = os.path.dirname(__file__)
JMX_SCRIPT_PATH=dir_name+"/../batscripts/execute_kafka_runclass.bat"