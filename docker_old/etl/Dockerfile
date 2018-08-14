FROM airdock/oracle-jdk:latest

WORKDIR /kf-etl
ENV ROOT_DIR /kf-etl

USER root

RUN mkdir lib/ conf/ bin/ data/

ARG SPARK_VERSION=2.3.1
#rtisma ARG SBT_VERSION=1.1.6
#rtisma ARG ETL_GIT_TAG=develop
ARG SPARK_MASTER="spark://localhost:7077"
ARG SPARK_DEPLOY_MODE=client



RUN apt-get update && apt-get install -y openssh-client openssh-server vim net-tools wget unzip iputils-ping telnet
RUN ssh-keygen -t rsa -N "" -f id_rsa
RUN mkdir /root/.ssh && mv id_rsa id_rsa.pub /root/.ssh/
RUN cd /root/.ssh && cat id_rsa.pub > authorized_keys

RUN update-rc.d ssh defaults
RUN echo PermitRootLogin yes >> /etc/ssh/sshd_config
RUN echo export JAVA_HOME=/srv/java/jdk >> /root/.bashrc


# Manually install SPARK
COPY smart-download-spark.sh .
ENV SPARK_NAME spark-${SPARK_VERSION}-bin-hadoop2.7
ENV SPARK_TGZ ${SPARK_NAME}.tgz
ENV SPARK_HOME ${ROOT_DIR}/spark
RUN echo "Installing spark..." && \
        ./smart-download-spark.sh ${SPARK_VERSION} && \
        tar xfz ${SPARK_TGZ} && \
        mv ${SPARK_NAME}/  ${SPARK_HOME} && \
        rm ${SPARK_TGZ}

ENV SPARK_HOME /kf-etl/spark
ENV JAVA_HOME /srv/java/jdk

COPY slaves /kf-etl/spark/conf/
COPY spark-env.sh /kf-etl/spark/conf/

#######################################################
## NOTE
######################################################
# Could not properly compile. will just copy jar over, 
# and comment out sbt installation, and kf-portal-etl compilation
#rtisma    COPY smart-download-sbt.sh .
#rtisma    ENV SBT_HOME ${ROOT_DIR}/sbt
#rtisma    ENV SBT_ZIP sbt-${SBT_VERSION}.zip
#rtisma    RUN echo "Installing SBT..." && \
#rtisma            ./smart-download-sbt.sh ${SBT_VERSION} && \
#rtisma        unzip ${SBT_ZIP} && \
#rtisma        rm -f ${SBT_ZIP} && \
#rtisma        ls -l ${SBT_HOME}
#rtisma    
#rtisma    
#rtisma    ENV ETL_HOME ${ROOT_DIR}/kf-portal-etl
#rtisma    ENV ETL_ZIP  .zip
#rtisma    RUN echo "Installing and compiling kf-portal-etl..." && \
#rtisma        wget https://github.com/kids-first/kf-portal-etl/archive/${ETL_GIT_TAG}.zip && \
#rtisma        unzip ${ETL_GIT_TAG}.zip && \
#rtisma        mv ${ETL_HOME}-${ETL_GIT_TAG} ${ETL_HOME} && \
#rtisma        rm -f ${ETL_GIT_TAG}.zip && \
#rtisma        cd ${ETL_HOME} && \
#rtisma        ../sbt/bin/sbt pipeline/assembly && \
#rtisma        find /kf-portal-etl/kf-portal-etl-pipeline/target/ -name "*.jar"


COPY kf-etl-submit.sh bin/

RUN chown -R root:root /kf-etl
RUN chmod +x /kf-etl/bin/kf-etl-submit.sh

ENV SPARK_MASTER=$SPARK_MASTER

# WARNING: these must be mounted externally
ENV ETL_JAR_MOUNT_PATH=/kf-etl/mnt/lib/kf-portal-etl.jar
ENV ETL_CONF_MOUNT_PATH=/kf-etl/mnt/conf/kf_etl.conf


ENV SPARK_DEPLOY_MODE=$SPARK_DEPLOY_MODE

# check the conf file is mounted
ENTRYPOINT ["/kf-etl/bin/kf-etl-submit.sh"]

EXPOSE 8080 4040 7077 6066 8081

# install oracle java 8 with silent option
#RUN apt-get install -y software-properties-common
#RUN add-apt-repository ppa:webupd8team/java
#RUN apt-get update
#RUN echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections
#RUN echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections
#RUN apt-get install -y oracle-java8-installer
#ENV JAVA_HOME /usr/lib/jvm/java-8-oracle
