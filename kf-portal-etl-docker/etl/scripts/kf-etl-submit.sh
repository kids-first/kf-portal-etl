#!/bin/bash

if [ ! -e ${ETL_CONF_FILE} ]; then
    echo "The ETL_CONF (${ETL_CONF_FILE}) does not exist. Check the file was mounted properly."
    exit 1
fi

if [  -z ${KF_STUDY_ID+x} ]; then
    echo "The env variable KF_STUDY_ID is undefined"
    exit 1
fi

if [  -z ${KF_DRIVER_MEMORY+x} ]; then
    KF_DRIVER_MEMORY="1g"
fi

if [  -z ${KF_RELEASE_ID+x} ]; then
    echo "The env variable KF_RELEASE_ID is undefined"
    exit 1
fi

/kf-etl/spark/bin/spark-submit --master local[*] --class io.kf.etl.ETLMain \
    --driver-memory ${KF_DRIVER_MEMORY} \
    ${KF_PORTAL_ETL_JAR} -study_id ${KF_STUDY_ID} -release_id ${KF_RELEASE_ID}

