# kf-portal-etl

The Kids-First ETL is built on Scala, Spark, Elasticsearch, HDFS, Postgresql, MySQL etc.

## Build

To build the application, run the following from the command line in the root directory of the project

```    sbt ";clean;assembly"   ```

Then get the application jar file from

```${root of the project}/kf-portal-etl-processors/target/scala-2.11/kf-portal-etl.jar ```

## Configuration

The Kids-First ETL uses [lightbend/config](https://github.com/lightbend/config) as the configuration library. [kf_etl.conf.sample](./kf-portal-etl-common/src/main/resources/kf_etl.conf.sample) defines all of the configuration objects used by ETL.

The top namespace of the configuration is `io.kf.etl`. To refer to any specific configuration object, please prefix the namespace string.

* `spark` defines how the ETL connects to Spark environment
* `postgresql` defines how the ETL connects to the PostgreSQL server, where the raw data to be transformed is stored
* `elasticsearch` defines how the ETL connects to the Elasticsearch environment
* `hdfs` defines how the ETL connects to the HDFS cluster.  The HDFS cluster is used by the ETL for intermediate data writing
    * `root`: defines the root path of the entire ETL dataset under HDFS 
* `processors` defines all of the processors. Processors are the basic execution units of the ETL. Each processor has its own configurations, which means a processor could refer to the top level configuration objects or could also customize extra configuration objects to complete its specific job. At runtime, the ETL `Context` will pass the specific configlet to each processor.
    * `download` dumps clinical data from PostgreSQL and HPO data from MySQL to `dump_path`
    * `file_centric` transforms and generates the data for the [file-centric](https://github.com/kids-first/kf-es-model/blob/master/es-model-archive/kf-es-model-latest/file_centric.mapping.json) index in Elasticsearch. 
        * `data_path` defines where the processor stores both intermediate and output data
        * `write_intermediate_data` defines if the processor will write the intermediate data into `data_path`. It is optional; if not available in the file, `false` will be the default value
    * `participant_entric` transforms and generates the data for the [participant-centric](https://github.com/kids-first/kf-es-model/blob/master/es-model-archive/kf-es-model-latest/participant_centric.mapping.json) index in Elasticsearch
    * `index` stores the generated data from the above processors in Elasticsearch
        * `release_tag` defines how to generate a release tag as the suffix of the index name. It is format-free, which means one could define any necessary configuration objects needed for the current implementation
            * `release_tag_class_name` is a full class name of the release_tag implementation class and is required. At runtime, the ETL will pass the configlet to this class.

The ETL has a system environment variable called `kf.etl.config` which accept a URL string. The following are supported as values:
* `classpath:///.../${file_name}`, URL scheme [classpath://](./kf-portal-etl-common/src/main/scala/io/kf/etl/common/url) is not defined in JDK, however theETL defines it. Remember to mixin [ClasspathURLEnabler](./kf-portal-etl-common/src/main/scala/io/kf/etl/common/url/ClasspathURLEnabler.scala) trait
* `file:///.../${file_name}`
* `http://${http_server_ip}/.../${file_name}`

If `kf.etl.config` is not provided when the application is submitted to Spark, the ETL will search the root of the class path for the default file with the name `kf_etl.conf`, otherwise the application will quit.

## Running the Application

There are some dependencies to run Kids-First ETL, refer to [submit.sh.example](submit.sh.example)

* PostgreSQL: primary storage for Kids-First data models
* MySQL: contains HPO reference data. The reference DB dump file is found [here](http://human-phenotype-ontology.github.io/downloads.html)
* Spark 2.2.1 
* Configuration file: refer to [Configuration](#Configuration) for the format and contents of the file

To submit the application to Spark, run the following in the command line

``` ${SPARK_HOME}/bin/spark-submit --master spark://${Spark master node name or IP}:7077 --deploy-mode cluster --class io.kf.etl.ETLMain --driver-java-options "-Dkf.etl.config=${URL string for configuration file}" --conf "spark.executor.extraJavaOptions=-Dkf.etl.config=${URL string for configuration file}" ${path to kf-portal-etl.jar} ```