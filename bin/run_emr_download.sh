#!/bin/bash
set -x
study_ids=${1:-"SD_46SK55A3,SD_7NQ9151J,SD_NMVV8A1Y,SD_BHJXBDQK,SD_EKWJJJQG,SD_YGVA0E1C,SD_9PYZAHHE,SD_ZXJFFMEF,SD_W0V965XZ,SD_DZTB5HRR,SD_6FPYJQBR,SD_DYPMEHHF"}
release_id=$2
instance_type=${3:-"m5.xlarge"}
instance_count=${4:-"1"}

steps=$(cat <<EOF
[
    {
        "Name":"Copy duocodes locally",
        "Args":["wget", "https://raw.githubusercontent.com/EBISPOT/DUO/master/src/ontology/duo.csv", "-O", "/home/hadoop/duo.csv"],
        "ActionOnFailure":"TERMINATE_CLUSTER",
        "Type":"CUSTOM_JAR",
        "Jar":"command-runner.jar"
    },
    {
        "Name":"Upload duocodes to s3",
        "Args":["aws", "s3", "cp", "/home/hadoop/duo.csv", "s3://kf-strides-variant-parquet-prd/ontologies/raw/duo_codes/duo.csv"],
        "ActionOnFailure":"TERMINATE_CLUSTER",
        "Type":"CUSTOM_JAR",
        "Jar":"command-runner.jar"
    },
    {
        "Name":"Cpy congi file",
        "Args":["aws","s3","cp","s3://kf-strides-variant-parquet-prd/jobs/conf/kf-etl.conf", "/home/hadoop"],
        "ActionOnFailure":"TERMINATE_CLUSTER",
        "Type":"CUSTOM_JAR",
        "Jar":"command-runner.jar"
    },
   {
      "Args":[
          "spark-submit","--deploy-mode","client",
          "--class","io.kf.etl.ETLDownloadOnlyMain",
          "s3a://kf-strides-variant-parquet-prd/jobs/kf-portal-etl.jar",
          "${study_ids}"
      ],
      "Type":"CUSTOM_JAR",
      "ActionOnFailure":"TERMINATE_CLUSTER",
      "Jar":"command-runner.jar",
      "Properties":"",
      "Name":"Spark application"
   }
]
EOF
)
instance_groups="[{\"InstanceCount\":${instance_count},\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${instance_type}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":32,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m5.xlarge\",\"Name\":\"Master - 1\"}]"

aws emr create-cluster --applications Name=Hadoop Name=Spark \
--ec2-attributes '{"KeyName":"flintrock","InstanceProfile":"kf-variant-emr-ec2-prd-profile","SubnetId":"subnet-00aab84919d5a44e2","ServiceAccessSecurityGroup":"sg-0587a1d20e24f4104","EmrManagedSlaveSecurityGroup":"sg-0dc6b48e674070821","EmrManagedMasterSecurityGroup":"sg-0a31895d33d1643da"}' \
--service-role kf-variant-emr-prd-role \
--enable-debugging \
--release-label emr-5.19.0 \
--log-uri 's3n://kf-strides-variant-parquet-prd/jobs/elasticmapreduce/' \
--steps "${steps}" \
--name "Download clinical data - Studies ${study_ids} - Release ${release_id}" \
--instance-groups "${instance_groups}" \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--auto-terminate \
--configurations file://./spark-config.json \
--region us-east-1
