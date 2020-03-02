"""
This is the file that will execute the JMX tool for fetching the kafka metrics based on the MBEAN provided
"""

import subprocess
import sys

from pyscripts.constants.app_consumer_constants import *


def run_command(filepath, KAFKA_HOME,MBEAN):
    process = subprocess.Popen([filepath, KAFKA_HOME,MBEAN], shell=True, stdout=subprocess.PIPE)
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print(output.strip())
    rc = process.poll()
    return rc


if __name__ == "__main__":
    if sys.argv[1] and sys.argv[2]:
        KAFKA_HOME=sys.argv[1]
        MBEAN=sys.argv[2]
        run_command(JMX_SCRIPT_PATH, KAFKA_HOME,MBEAN)
