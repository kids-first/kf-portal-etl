FROM mysql/mysql-server:5.5.60-1.1.5

# environment variables MYSQL_ROOT_PASSWORD and MYSQL_DATABASE are inherited from mysql:5.5.60

ARG HPO_SQL_FILE_NAME=MYHPO_03_2018.sql

RUN yum install -y wget unzip procps iputils

#RUN wget http://compbio.charite.de/jenkins/job/hpo.annotations.monthly/lastSuccessfulBuild/artifact/annotation/${HPO_SQL_FILE_NAME}
COPY ${HPO_SQL_FILE_NAME} /docker-entrypoint-initdb.d/


