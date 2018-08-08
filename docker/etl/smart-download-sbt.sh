#! /bin/bash

SBT_VERSION=$1

SBT_FILENAME=sbt-${SBT_VERSION}.zip
SBT_DOWNLOAD_PATH=https://piccolo.link/sbt-${SBT_VERSION}.zip

if [ ! -e $SBT_FILENAME ]; then
    wget $SBT_DOWNLOAD_PATH
else 
    echo "The file '${SBT_FILENAME}' already exists. Skipping download."
fi
