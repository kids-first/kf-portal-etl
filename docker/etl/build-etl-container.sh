#!/bin/bash

imagename="kids-first/etl"
imageversion="0.0.1"

docker build --build-arg SPARK_VERSION=2.3.0 -t ${imagename}:${imageversion} .