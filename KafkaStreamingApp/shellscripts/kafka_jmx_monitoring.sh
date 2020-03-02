echo $1
KAFKA_HOME=$1
MBEAN=$2
${KAFKA_HOME}\bin\windows\kafka-run-class.bat kafka.tools.JmxTool --object-name ${MBEAN}

