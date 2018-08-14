#! /bin/bash

SPARK_VERSION=$1

HASH_TYPE=sha512
SPARK_FILENAME=spark-${SPARK_VERSION}-bin-hadoop2.7.tgz
SPARK_DOWNLOAD_ROOT_PATH=http://apache.mirror.iweb.ca
SPARK_DOWNLOAD_PATH=${SPARK_DOWNLOAD_ROOT_PATH}/spark/spark-${SPARK_VERSION}/${SPARK_FILENAME}
SPARK_HASH_PATH=${SPARK_DOWNLOAD_ROOT_PATH}/spark/spark-${SPARK_VERSION}/${SPARK_FILENAME}.${HASH_TYPE}

if [ ! -e $SPARK_FILENAME ]; then
    wget $SPARK_DOWNLOAD_PATH
else
    wget $SPARK_HASH_PATH -O expected-spark-hash.txt
    gpg --print-md $HASH_TYPE $SPARK_FILENAME > actual-spark-hash.txt
    result=$(diff expected-spark-hash.txt actual-spark-hash.txt)
    if [ "$result" != "" ]; then
        rm -f $SPARK_FILENAME
        wget $SPARK_DOWNLOAD_PATH
    fi 
fi
