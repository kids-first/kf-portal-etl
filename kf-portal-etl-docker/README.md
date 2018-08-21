# How to run

Kids-First ETL docker environment uses docker-compose to start 4 docker containers:
* kf-etl
* kf-hpo
* kf-es
* kf-hadoop

By default, docker-compose sets up a single network for ETL environment. Each container joins the default network and is reachable by other containers and discoverable by them through hostname

# kf-etl

The directory ```etl``` defines the ```kf-etl``` container, which has embedded single-node Spark cluster  

### Building
The ETL Dockerfile asks for the following `ARG` to be provided when building the docker image:

- `SPARK_VERSION`: Spark version. There is a default value
- `SBT_VERSION`: SBT version. There is a default value
- `SHADE_VERSION`: Version of the dependent scalapb-json4s-shade scala library 
- `SPARK_MASTER`: Spark cluster master URL.
- `SPARK_DEPLOY_MODE`: Spark application deployment mode, the default value is `client`
- `ETL_VERSION`: Git branch/commit-id/tag of the kf-portal-etl/kf-portal-etl-docker/etl/Dockerfile to build. Default is develop
- `ES_MODEL_VERSION`: Got branch/commit-id/tag of the kf-es-model repositry that will be used. Default is master

### Running the container

The file `kf-etl-submit.sh` is the ETL container's entry point. It starts `sshd` service first, then start Spark `master` and `worker` nodes, then submit ETL application. There are a few prequisites when running the container:

1. Mount point
The container must be started with a kf_etl.conf file from the docker host mounted on `/kf-etl/mnt/conf/kf_etl.conf` in the container. The `kf-etl-submit.sh` script uses the kf_etl.conf file from that fixed path.
For example, if the absolute path to the .conf file from the docker host that is to be used is `/home/ubuntu/my_kf_etl.conf` then the `docker run` parameter for mounting would be:

```bash
-v /home/ubuntu/my_kf_etl.conf:/kf-etl/mnt/conf/kf_etl.conf

```

Additionally, you must ensure all the urls defined in the kf_etl.conf, will be accessible from with in the container when it runs.

2. ENV Variables
The container requires the release and studyies to be defined. This is passed to the container using environement variables. You can define the release and mutliple studies with the following `docker run` arguments:

```bash
-e "KF_RELEASE_ID=my_release_id"
-e "KF_STUDY_ID=my_study_1 my_study_2"

```

# kf-hpo

The directory `external/hpo` defines the `kf-hpo` containers, which has a mysql database named `HPO` populated with HPO reference data. 

The `etl` container refers to it through the hostname `kf-hpo`

# Other dependencies

In addition to `kf-hpo`, the kf-etl container depends on the dataservice, elasticsearch, rollcall, release-coordinator, and hadoop.

# kf_etl.conf

A specific version of ETL configuration file for the docker environment. The hostnames of the containers are used to refer to the corresponding services

# In Production

In production, a separate Spark cluster, a separate Elasticsearch cluster and a separate HDFS cluster exist. So the corresponding configuration values have to be changed to reflect the new environment. When running Kids-First ETL, we can only keep `kf-etl` and `kf-hpo`

Further, Jenkins could maintain the `kf_etl.conf` and add it into the assembly jar when building the ETL. In this case, We can remove `-Dkf.etl.config=${path/to/file}` from the starting script.
