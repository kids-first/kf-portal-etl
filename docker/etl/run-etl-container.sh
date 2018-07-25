#!/bin/bash

host_jar_location="../../kf-portal-etl-pipeline/target/scala-2.11/kf-portal-etl.jar"
host_conf_location="../kf_etl.conf"
containername="kf-etl"
imagename="kids-first/etl"
imageversion="0.0.1"
ports=( 4040 6066 7077 8080 8081 )
port_cmd=""
for port in ${ports[*]}; do
    port_cmd+=" -p ${port}:${port} "
done

docker run \
    --rm \
    -v "${host_jar_location}:/kf-etl/mnt/lib/kf-portal-etl.jar" \
    -v "${host_conf_location}:/kf-etl/mnt/conf/kf_etl.conf" \
    ${port_cmd} \
    --name $containername \
    ${imagename}:${imageversion}

