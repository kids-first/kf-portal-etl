# How to run

Kids-First ETL docker environment uses docker-compose to start 4 docker containers:
* kf-etl
* kf-hpo
* kf-es
* kf-hadoop

By default, docker-compose sets up a single network for ETL environment. Each container joins the default network and is reachable by other containers and discoverable by them through hostname

The shell script ```run-etl-no-build.sh``` refers to the file ```docker-compose-no-build.yml``` and starts the ETL environment by assuming container images are already built

the shell script ```run-etl-with-build.sh``` refers to the file ```docker-compose-with-build.yml``` and starts the ETL environment by building the container images first

# kf-etl

The directory ```etl``` defines the ```kf-etl``` container, which has embedded single-node Spark cluster  

The ETL Dockerfile asks for the following `ARG` to be provided when building the docker image:

- `SPARK_VERSION`: Spark version. The default value is `2.3.0`
- `ETL_JAR_URL`: ETL jar file in the URL format, for example: http://..../kf-portal-etl.jar. The default value is `kf-portal-etl.jar`
- `ETL_CONFIGURATION_FILE_URL`: ETL configuration file in the URL format, for example: http://..../kf_etl.conf. The default value is `kf_etl.conf`
- `SPARK_MASTER`: Spark cluster master URL.
- `SPARK_DEPLOY_MODE`: Spark application deployment mode, the default value is `client`

The file `kf-etl-submit.sh` is the ETL container's entry point. It starts `sshd` service first, then start Spark `master` and `worker` nodes, then submit ETL application

# kf-hpo

The directory `hpo` defines the `kf-hpo` containers, which has a mysql database named `HPO` populated with HPO reference data. 

The `etl` container refers to it through the hostname `kf-hpo`

# kf-es

The directory `elasticsearch` defines the `kf-es` container, which has an Elasticsearch instance. 

Please check the docker-compose file about what environment variables are needed, and make sure the values should be the same in the configuration file 

The `etl` container refers to it through the hostname `kf-es`

# kf-hadoop

The directory `hadoop` defines the `kf-hadoop` container, which provides HDFS entry point which could be used to write the intermediate data into

# kf_etl.conf

A specific version of ETL configuration file for the docker environment. The hostnames of the containers are used to refer to the corresponding services

# In Production

In production, a separate Spark cluster, a separate Elasticsearch cluster and a separate HDFS cluster exist. So the corresponding configuration values have to be changed to reflect the new environment. When running Kids-First ETL, we can only keep `kf-etl` and `kf-hpo`

Further, Jenkins could maintain the `kf_etl.conf` and add it into the assembly jar when building the ETL. In this case, We can remove `-Dkf.etl.config=${path/to/file}` from the starting script.