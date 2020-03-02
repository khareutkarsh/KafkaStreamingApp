#!/bin/bash
. ../config/script_conf
${KAFKA_HOME}\bin\kafka-run-class.sh kafka.tools.JmxTool --object-name ${MBEAN}

