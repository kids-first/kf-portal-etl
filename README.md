<p align="center">
  <img src="docs/portal_etl.svg" alt="Kids First Portal ETL" width="660px">
</p>

# kf-portal-etl

The Kids-First ETL is built on Scala, Spark, Elasticsearch.


## Dependencies

Before [building](#build) this application, the following dependencies must be built and added to your local maven (.m2) directory.

### ES Model

1.  Clone from [repository](https://github.com/kids-first/kf-es-model)

    ```
    git clone git@github.com:kids-first/kf-es-model.git
    ```

1.  Maven install

    ```
    cd kf-es-model
    mvn install
    ```

## Build

To build the application, run the following from the command line in the root directory of the project

```
sbt ";clean;assembly"
```

Then get the application jar file from

```
${root of the project}/target/scala-2.11/kf-portal-etl.jar
```

## Configuration

The Kids-First ETL uses [lightbend/config](https://github.com/lightbend/config) as the configuration library. [kf_etl.conf](./kf-portal-etl-docker/kf_etl.conf) defines all of the configuration objects used by ETL.

The top namespace of the configuration is `io.kf.etl`. To refer to any specific configuration object, please prefix the namespace string.

- `spark` defines how the ETL connects to Spark environment
- `elasticsearch` defines how the ETL connects to the Elasticsearch environment
- `processors` defines all of the processors. Processors are the basic execution units of the ETL. Each processor has its own configurations, which means a processor could refer to the top level configuration objects or could also customize extra configuration objects to complete its specific job. 
  - `download` dumps clinical data from PostgreSQL and HPO data from MySQL to `dump_path`
  - `file_centric` transforms and generates the data for the [file-centric](https://github.com/kids-first/kf-es-model/blob/master/es-model-archive/kf-es-model-latest/file_centric.mapping.json) index in Elasticsearch.
    - `data_path` defines where the processor stores both intermediate and output data
    - `write_intermediate_data` defines if the processor will write the intermediate data into `data_path`. It is optional; if not available in the file, `false` will be the default value
  - `participant_entric` transforms and generates the data for the [participant-centric](https://github.com/kids-first/kf-es-model/blob/master/es-model-archive/kf-es-model-latest/participant_centric.mapping.json) index in Elasticsearch
  - `index` stores the generated data from the above processors in Elasticsearch
  
The ETL has a system environment variable called `kf.etl.config` which define the path to the configuration file.

If `kf.etl.config` is not provided when the application is submitted to Spark, the ETL will search the root of the class path for the default file with the name `kf_etl.conf`, otherwise the application will quit.

## Running the Application

There are some dependencies to run Kids-First ETL, refer to [submit.sh.example](submit.sh.example)

- Spark 2.3.4
- Configuration file: refer to [Configuration](#Configuration) for the format and contents of the file

To submit the application to Spark, run the following in the command line

`${SPARK_HOME}/bin/spark-submit --master spark://${Spark master node name or IP}:7077 --deploy-mode cluster --class io.kf.etl.ETLMain --driver-java-options "-Dkf.etl.config=${URL string for configuration file}" --conf "spark.executor.extraJavaOptions=-Dkf.etl.config=${URL string for configuration file}" ${path to kf-portal-etl.jar} ${command-line arguments}`

## Command line arguments

Kids-First ETL supports command-line argument `-study_id id1 id2`. In this case, ETL will filter the dataset retrieved from data service through `study_id`s.

The third command-line argument is `-release_id rid`

To submit the application with `study_id`s, run the following:

```
${SPARK_HOME}/bin/spark-submit --master spark://${Spark master node name or IP}:7077 --deploy-mode cluster --class io.kf.etl.ETLMain --driver-java-options "-Dkf.etl.config=${URL string for configuration file}" --conf "spark.executor.extraJavaOptions=-Dkf.etl.config=${URL string for configuration file}" ${path to kf-portal-etl.jar} -study_id id1 id2 -release_id rid
```
