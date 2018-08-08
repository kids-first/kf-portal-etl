#!/bin/bash

service ssh start

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

ETL_JAR_PATH=/kf-etl/lib/kf-portal-etl.jar
ETL_CONF_PATH=/kf-etl/conf/kf_etl.conf
ETL_JAR_URL=file://${ETL_JAR_PATH}
ETL_CONF_URL=file://${ETL_CONF_PATH}

# Make a copy of the mounted config
cp -fv ${ETL_CONF_MOUNT_PATH} ${ETL_CONF_PATH}

cp -fv ${ETL_JAR_MOUNT_PATH} ${ETL_JAR_PATH}

while [ $(ps axu|grep sshd|grep -v grep|wc -l) -eq 0 ]
do
    echo wait for 5s
    sleep 5s
done

ps aux|grep sshd

. /kf-etl/spark/sbin/start-all.sh

KF_ETL_CONFIG="-Dkf.etl.config=${ETL_CONF_URL}"

/kf-etl/spark/bin/spark-submit --master ${SPARK_MASTER} --deploy-mode ${SPARK_DEPLOY_MODE} --class io.kf.etl.ETLMain --driver-java-options "${KF_ETL_CONFIG}" --conf "spark.executor.extraJavaOptions=${KF_ETL_CONFIG}" ${ETL_JAR_URL} -study_id  ${KF_STUDY_ID} -release_id ${KF_RELEASE_ID}

