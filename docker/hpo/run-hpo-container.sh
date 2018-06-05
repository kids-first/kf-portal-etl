#!/bin/bash

containername="kf-hpo-mysql"
imagename="kids-first/hpo"
imageversion="0.0.1"

sudo docker run --name $containername --network host  -d -e MYSQL_ROOT_PASSWORD=12345 -e MYSQL_DATABASE=HPO ${imagename}:${imageversion}
