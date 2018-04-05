FROM airdock/oracle-jdk:latest

WORKDIR /kf-etl

ARG ETL_JAR_URL
ARG ETL_CONFIGURATION_FILE_URL
ARG SPARK_MASTER

ADD http://httpd-mirror.sergal.org/apache/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz .

#RUN apt-get update && apt-get upgrade && apt-get install -y wget
#
## download spark 2.3.0 from http://httpd-mirror.sergal.org/apache/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz
#RUN wget http://httpd-mirror.sergal.org/apache/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz
#
RUN tar xfvz spark-2.3.0-bin-hadoop2.7.tgz && mv spark-2.3.0-bin-hadoop2.7/ spark/ && rm spark-2.3.0-bin-hadoop2.7.tgz

RUN mkdir lib/ conf/

ADD ${ETL_JAR_URL} ./lib

ADD ${ETL_CONFIGURATION_FILE_URL} ./conf

COPY kf-etl-submit.sh .

ENTRYPOINT ["/kf-etl/kf-etl-submit.sh", "$SPARK_MASTER"]

