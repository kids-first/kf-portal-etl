#!/bin/bash

containername="kf-etl"
imagename="kids-first/etl"
imageversion="0.0.1"

docker run --name $containername -d ${imagename}:${imageversion}