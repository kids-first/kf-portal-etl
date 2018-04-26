#!/bin/bash

KF_ETL_CONFIG="-Dkf.etl.config=${ETL_CONFIGURATION_FILE_URL}"

/kf-etl/spark/bin/spark-submit --master ${SPARK_MASTER} --deploy-mode ${SPARK_DEPLOY_MODE} --class io.kf.etl.ETLMain --driver-java-options "${KF_ETL_CONFIG}" --conf "spark.executor.extraJavaOptions=${KF_ETL_CONFIG}" ${ETL_JAR_URL}

