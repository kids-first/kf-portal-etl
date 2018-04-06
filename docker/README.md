# ETL Dockerfile

The ETL Dockerfile asks for the following `ARG` to be provided when building the docker image:

- `SPARK_VERSION`: Spark version
- `ETL_JAR_URL`: ETL jar file in the URL format, for example: http://..../kf-portal-etl.jar
- `ETL_CONFIGURATION_FILE_URL`: ETL configuration file in the URL format, for example: http://..../kf_etl.conf
- `SPARK_MASTER`: Spark cluster master URL


# kf-etl-submit.sh

This file is used to submit ETL to the spark cluster