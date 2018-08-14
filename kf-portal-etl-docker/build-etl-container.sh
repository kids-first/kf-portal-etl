#!/bin/bash

imagename="kids-first/etl"
imageversion="0.0.1"
sparkmaster="spark://localhost:7077"

docker build  --build-arg SPARK_MASTER=${sparkmaster} -t ${imagename}:${imageversion} -f etl/Dockerfile etl/
