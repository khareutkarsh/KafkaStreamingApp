#!/bin/bash
. ../config/script_conf
python ../pyscripts/monitoring/kafka_jmx_monitoring.py KAFKA_HOME MBEAN
