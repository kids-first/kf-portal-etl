FROM maven:3.5.4-jdk-8

###########################
# Docker Build config
###########################
ARG SPARK_VERSION=2.3.3
ARG SBT_VERSION=1.1.6
ARG ETL_VERSION=develop
ARG ES_MODEL_VERSION=master
ARG SHADE_VERSION=master
ARG SPARK_MASTER="spark://localhost:7077"
ARG SPARK_DEPLOY_MODE=client
EXPOSE 8080 4040 7077 6066 8081

###########################
# Setup Build Environment
###########################
WORKDIR /kf-etl
ENV ROOT_DIR /kf-etl
USER root
COPY scripts/build_github_repo.sh .
RUN mkdir lib/ conf/ bin/ data/
RUN apt-get update && \
    apt-get install -y openssh-client openssh-server vim net-tools wget unzip iputils-ping telnet

###########################
# Install kf-portal-etl and its dependencies
###########################

# SBT Config
ENV SBT_DOWNLOAD_PATH https://piccolo.link/sbt-${SBT_VERSION}.zip
ENV SBT_HOME ${ROOT_DIR}/sbt
ENV SBT_ZIP sbt-${SBT_VERSION}.zip
ENV SBT_EXE ${SBT_HOME}/bin/sbt

# Shade Config
ENV SHADE_GIT_TAG ${SHADE_VERSION}
ENV SHADE_REPO_NAME scalapb-json4s-shade

# kf-es-model config
ENV ES_MODEL_GIT_TAG ${ES_MODEL_VERSION}
ENV ES_MODEL_REPO_NAME kf-es-model

# kf-portal-etl config
ENV ETL_GIT_TAG ${ETL_VERSION}
ENV ETL_REPO_NAME kf-portal-etl
ENV KF_PORTAL_ETL_JAR ${ROOT_DIR}/data/kf-portal-etl.jar

###########################
# Configure SSH
###########################
RUN ssh-keygen -t rsa -N "" -f id_rsa
RUN mkdir /root/.ssh && mv id_rsa id_rsa.pub /root/.ssh/
RUN cd /root/.ssh && cat id_rsa.pub > authorized_keys
RUN update-rc.d ssh defaults
RUN echo PermitRootLogin yes >> /etc/ssh/sshd_config

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

COPY spark_config/slaves ${SPARK_HOME}/conf/
COPY spark_config/spark-env.sh ${SPARK_HOME}/conf/
COPY scripts/kf-etl-submit.sh ${ROOT_DIR}/bin/
# RUN chown -R root:root ${ROOT_DIR} #rtisma hangs for a really long time...exclude for now
RUN chmod +x ${ROOT_DIR}/bin/kf-etl-submit.sh

###########################
# Configure Run command
###########################
ENV ETL_JAR_MOUNT_PATH=${KF_PORTAL_ETL_JAR}
# WARNING: this must be mounted externally
ENV ETL_CONF_MOUNT_PATH=/kf-etl/mnt/conf/kf_etl.conf
ENTRYPOINT ["/kf-etl/bin/kf-etl-submit.sh"]

