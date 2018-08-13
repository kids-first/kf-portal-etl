#!/bin/bash

host_jar_location=`readlink -f ../../kf-portal-etl-pipeline/target/scala-2.11/kf-portal-etl.jar`
host_conf_location=`readlink -f ../kf_etl.conf`
containername="kf-etl"
imagename="kids-first/etl"
imageversion="0.0.1"
ports=( 4040 6066 7077 8080 8081 )
port_cmd=""
kf_study_id="SD_9PYZAHHE SD_YGVA0E1C"
kf_release_id="RE_ACE14FCS"
#network="default"
network="docker_default"
for port in ${ports[*]}; do
    port_cmd+=" -p ${port}:${port} "
done

docker run \
    --rm \
    -e KF_STUDY_ID="${kf_study_id}" \
    -e KF_RELEASE_ID="${kf_release_id}" \
    --net=${network} \
    -v "${host_jar_location}:/kf-etl/mnt/lib/kf-portal-etl.jar" \
    -v "${host_conf_location}:/kf-etl/mnt/conf/kf_etl.conf" \
    ${port_cmd} \
    --name $containername \
    ${imagename}:${imageversion}

