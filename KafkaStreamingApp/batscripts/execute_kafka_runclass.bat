set KAFKA_HOME="F:\kafka_2.12-2.4.0"
set MBEAN="kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec"

%KAFKA_HOME%\bin\windows\kafka-run-class.bat kafka.tools.JmxTool --object-name %MBEAN%

