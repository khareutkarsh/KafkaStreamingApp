echo %1
set KAFKA_HOME=%1
set MBEAN=%2
%KAFKA_HOME%\bin\windows\kafka-run-class.bat kafka.tools.JmxTool --object-name %MBEAN%

