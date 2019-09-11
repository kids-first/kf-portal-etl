FROM openjdk:8

###########################
# Docker Build config
###########################
ARG SPARK_VERSION=2.3.4
EXPOSE 8080 4040 7077 6066 8081

###########################
# Setup Build Environment
###########################
WORKDIR /kf-etl
ENV ROOT_DIR /kf-etl
USER root
RUN mkdir conf/ bin/ data/
RUN apt-get update && \
    apt-get install -y wget unzip

###########################
# Install Spark
###########################
ENV SPARK_HOME ${ROOT_DIR}/spark
ENV SPARK_HASH_TYPE sha512
ENV SPARK_DOWNLOAD_ROOT_PATH http://apache.mirror.iweb.ca
ENV SPARK_NAME spark-${SPARK_VERSION}-bin-hadoop2.7
ENV SPARK_FILENAME ${SPARK_NAME}.tgz
ENV SPARK_HASH_PATH ${SPARK_DOWNLOAD_ROOT_PATH}/spark/spark-${SPARK_VERSION}/${SPARK_FILENAME}.${SPARK_HASH_TYPE}
ENV SPARK_DOWNLOAD_PATH ${SPARK_DOWNLOAD_ROOT_PATH}/spark/spark-${SPARK_VERSION}/${SPARK_FILENAME}
RUN echo "Installing spark..." && \
        wget ${SPARK_DOWNLOAD_PATH} && \
        tar xfz ${SPARK_FILENAME} && \
        mv ${SPARK_NAME}/  ${SPARK_HOME} && \
        rm ${SPARK_FILENAME}

ADD kf-portal-etl-docker/etl/spark_config/slaves ${SPARK_HOME}/conf/
ADD kf-portal-etl-docker/etl/spark_config/spark-env.sh ${SPARK_HOME}/conf/
ADD kf-portal-etl-docker/etl/scripts/kf-etl-submit.sh ${ROOT_DIR}/bin/
RUN chmod +x ${ROOT_DIR}/bin/kf-etl-submit.sh

###########################
# Configure Run command
###########################
ENV KF_PORTAL_ETL_JAR ${ROOT_DIR}/data/kf-portal-etl.jar
# WARNING: this must be mounted externally
ENV ETL_CONF_MOUNT_PATH /kf-etl/conf/kf_etl.conf

ADD kf-portal-etl-pipeline/target/scala-2.11/kf-portal-etl.jar ${KF_PORTAL_ETL_JAR}

ENTRYPOINT ["/kf-etl/bin/kf-etl-submit.sh"]

