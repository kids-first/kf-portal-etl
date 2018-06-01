FROM airdock/oracle-jdk:latest

USER root

ARG SPARK_VERSION=2.3.0

COPY spark-${SPARK_VERSION}-bin-hadoop2.7.tgz /usr/local

RUN cd /usr/local && tar xfz spark-${SPARK_VERSION}-bin-hadoop2.7.tgz && mv spark-${SPARK_VERSION}-bin-hadoop2.7/ spark/ && rm spark-${SPARK_VERSION}-bin-hadoop2.7.tgz

ENV SPARK_HOME /usr/local/spark
ENV JAVA_HOME /srv/java/jdk

COPY ../etl/slaves /usr/local/spark/conf/
COPY spark-env.sh /usr/local/spark/conf/

COPY start_spark.sh /usr/local/bin/start_spark.sh
RUN chown root:root /usr/local/bin/start_spark.sh
RUN chmod +x /usr/local/bin/start_spark.sh

RUN apt-get update && apt-get install -y openssh-client openssh-server vim
RUN ssh-keygen -t rsa -N "" -f id_rsa
RUN mkdir .ssh && mv id_rsa id_rsa.pub .ssh/
RUN cd .ssh && cat id_rsa.pub > authorized_keys

RUN update-rc.d ssh defaults
RUN echo PermitRootLogin yes >> /etc/ssh/sshd_config
RUN echo export JAVA_HOME=/srv/java/jdk >> .bashrc


ENTRYPOINT ["/usr/local/bin/start_spark.sh"]

EXPOSE 8080 4040 7077 6066 8081