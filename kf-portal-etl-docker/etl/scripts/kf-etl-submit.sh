#!/bin/bash


if [ ! -e ${ETL_CONF_MOUNT_PATH} ]; then
    echo "The ETL_CONF_MOUNT_PATH (${ETL_CONF_MOUNT_PATH}) does not exist. Check the file was mounted properly."
    exit 1
fi

if [ ! -e ${ETL_JAR_MOUNT_PATH} ]; then
    echo "The ETL_JAR_MOUNT_PATH (${ETL_JAR_MOUNT_PATH}) does not exist. Check the file was mounted properly."
    exit 1
fi

if [  -z ${KF_STUDY_ID+x} ]; then
    echo "The env variable KF_STUDY_ID is undefined"
    exit 1
fi

if [  -z ${KF_RELEASE_ID+x} ]; then
    echo "The env variable KF_RELEASE_ID is undefined"
    exit 1
fi


KF_ETL_CONFIG="-Dkf.etl.config=file://${ETL_CONF_MOUNT_PATH}"
/kf-etl/spark/bin/spark-submit --master local[*] --class io.kf.etl.ETLMain \
    --driver-java-options "${KF_ETL_CONFIG}" \
    --conf "spark.executor.extraJavaOptions=${KF_ETL_CONFIG}" \
    --conf "spark.executor.memory=2g" \
    ${KF_PORTAL_ETL_JAR} -study_id  ${KF_STUDY_ID} -release_id ${KF_RELEASE_ID}

