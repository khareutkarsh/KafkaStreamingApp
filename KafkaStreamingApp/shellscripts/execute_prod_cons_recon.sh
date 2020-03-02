#!/bin/bash
. ../config/script_conf
cd ../dist
python ${EGG_FILE_NAME} PROD_CONS_RECON ${PRODUCER_NAME} ${DB_FILE_PATH} ${OUTPUT_FILE_DIR}