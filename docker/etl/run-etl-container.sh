#!/bin/bash

containername="kf-etl"
imagename="kids-first/etl"
imageversion="0.0.1"

sudo docker run --name $containername --network host ${imagename}:${imageversion}

