#!/bin/bash

cd ..

sbt pipeline/assembly

cp kf-portal-etl-pipeline/target/scala-2.11/kf-portal-etl.jar ./docker/etl
cd docker/
cp kf_etl.conf ./etl

docker-compose -f docker-compose.yml up -d

rm kf-portal-etl.jar
rm kf_etl.conf