#!/bin/bash

/kf-etl/spark/bin/spark-submit --master $1 --deploy-mode cluster --class io.kf.etl.ETLMain --driver-java-options "-Dkf.etl.config=file:///kf-etl/conf/kf_etl.conf" --conf "spark.executor.extraJavaOptions=-Dkf.etl.config=file:///kf-etl/conf/kf_etl.conf" /kf-etl/lib/kf-portal-etl.jar

