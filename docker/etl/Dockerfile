FROM airdock/oracle-jdk:latest

WORKDIR /kf-etl

USER root

RUN mkdir lib/ conf/ bin/ data/

ARG SPARK_VERSION=2.3.0
ARG SPARK_MASTER
ARG SPARK_DEPLOY_MODE=client
ARG ETL_JAR_FILE_NAME=kf-portal-etl.jar
ARG ETL_CONF_FILE_NAME=kf_etl.conf

COPY spark-${SPARK_VERSION}-bin-hadoop2.7.tgz .

RUN tar xfz spark-${SPARK_VERSION}-bin-hadoop2.7.tgz && mv spark-${SPARK_VERSION}-bin-hadoop2.7/ spark/ && rm spark-${SPARK_VERSION}-bin-hadoop2.7.tgz

ENV SPARK_HOME /kf-etl/spark
ENV JAVA_HOME /srv/java/jdk

COPY slaves /kf-etl/spark/conf/
COPY spark-env.sh /kf-etl/spark/conf/

RUN apt-get update && apt-get install -y openssh-client openssh-server vim
RUN ssh-keygen -t rsa -N "" -f id_rsa
RUN mkdir /root/.ssh && mv id_rsa id_rsa.pub /root/.ssh/
RUN cd /root/.ssh && cat id_rsa.pub > authorized_keys

RUN update-rc.d ssh defaults
RUN echo PermitRootLogin yes >> /etc/ssh/sshd_config
RUN echo export JAVA_HOME=/srv/java/jdk >> /root/.bashrc

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

EXPOSE 8080 4040 7077 6066 8081