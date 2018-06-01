#!/bin/bash

service ssh start

. /kf-etl/spark/sbin/start-all.sh

KF_ETL_CONFIG="-Dkf.etl.config=${ETL_CONF_URL}"

echo ${ETL_JAR_URL}

if [ -f "${ETL_JAR_URL}" ]; then
    echo "existing"
else
    echo "not existing"
fi

/kf-etl/spark/bin/spark-submit --master ${SPARK_MASTER} --deploy-mode ${SPARK_DEPLOY_MODE} --class io.kf.etl.ETLMain --driver-java-options "${KF_ETL_CONFIG}" --conf "spark.executor.extraJavaOptions=${KF_ETL_CONFIG}" ${ETL_JAR_URL}

