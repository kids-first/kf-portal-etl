FROM airdock/oracle-jdk:latest

WORKDIR /kf-etl

USER root

RUN mkdir lib/ conf/ bin/ data/

ARG SPARK_VERSION=2.3.0
ARG SPARK_MASTER
ARG SPARK_DEPLOY_MODE=cluster
ARG ETL_JAR_FILE_NAME=kf-portal-etl.jar
ARG ETL_CONF_FILE_NAME=kf_etl.conf

ADD http://httpd-mirror.sergal.org/apache/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz .

RUN tar xfz spark-${SPARK_VERSION}-bin-hadoop2.7.tgz && mv spark-${SPARK_VERSION}-bin-hadoop2.7/ spark/ && rm spark-${SPARK_VERSION}-bin-hadoop2.7.tgz

COPY kf-etl-submit.sh bin/
COPY $ETL_JAR_FILE_NAME lib/
COPY $ETL_CONF_FILE_NAME conf/

RUN chown -R root:root /kf-etl
RUN chmod +x /kf-etl/bin/kf-etl-submit.sh

ENV SPARK_MASTER=$SPARK_MASTER
ENV ETL_JAR_URL=file:///kf-etl/lib/$ETL_JAR_FILE_NAME
ENV ETL_CONF_URL=file:///kf-etl/conf/$ETL_CONF_FILE_NAME
ENV SPARK_DEPLOY_MODE=$SPARK_DEPLOY_MODE

ENTRYPOINT ["/kf-etl/bin/kf-etl-submit.sh"]
