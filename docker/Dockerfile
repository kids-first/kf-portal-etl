FROM airdock/oracle-jdk:latest

WORKDIR /kf-etl

USER root

ARG SPARK_VERSION
ARG ETL_JAR_URL
ARG ETL_CONFIGURATION_FILE_URL
ARG SPARK_MASTER

ADD http://httpd-mirror.sergal.org/apache/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz .

RUN tar xfz spark-${SPARK_VERSION}-bin-hadoop2.7.tgz && mv spark-${SPARK_VERSION}-bin-hadoop2.7/ spark/ && rm spark-${SPARK_VERSION}-bin-hadoop2.7.tgz

RUN mkdir lib/ conf/

#ADD $ETL_JAR_URL lib/

#ADD $ETL_CONFIGURATION_FILE_URL conf/

COPY kf-etl-submit.sh .

RUN chown -R root:root /kf-etl
RUN chmod +x /kf-etl/kf-etl-submit.sh

ENV SPARK_MASTER=$SPARK_MASTER
ENV ETL_JAR_URL=$ETL_JAR_URL
ENV ETL_CONFIGURATION_FILE_URL=$ETL_CONFIGURATION_FILE_URL

#ENTRYPOINT ["/kf-etl/kf-etl-submit.sh"]

RUN ls -al /kf-etl
RUN ls -al /kf-etl/lib
RUN ls -al /kf-etl/conf
RUN cat /kf-etl/kf-etl-submit.sh