FROM airdock/oracle-jdk:latest

WORKDIR /kf-etl

USER root

ARG SPARK_VERSION=2.2.1
ARG ETL_JAR_URL
ARG ETL_CONFIGURATION_FILE_URL
ARG SPARK_MASTER
ARG SPARK_DEPLOY_MODE=client

ADD http://httpd-mirror.sergal.org/apache/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz .

RUN tar xfz spark-${SPARK_VERSION}-bin-hadoop2.7.tgz && mv spark-${SPARK_VERSION}-bin-hadoop2.7/ spark/ && rm spark-${SPARK_VERSION}-bin-hadoop2.7.tgz

COPY kf-etl-submit.sh .

RUN chown -R root:root /kf-etl
RUN chmod +x /kf-etl/kf-etl-submit.sh

ENV SPARK_MASTER=$SPARK_MASTER
ENV ETL_JAR_URL=$ETL_JAR_URL
ENV ETL_CONFIGURATION_FILE_URL=$ETL_CONFIGURATION_FILE_URL
ENV SPARK_DEPLOY_MODE=$SPARK_DEPLOY_MODE

ENTRYPOINT ["/kf-etl/kf-etl-submit.sh"]
