#!/bin/bash

containername="kf-etl"
imagename="kids-first/etl"
imageversion="0.0.1"

sudo docker run --hostname $containername --name $containername ${imagename}:${imageversion}

