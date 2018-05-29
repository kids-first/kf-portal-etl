#!/bin/bash

imagename="kids-first/hpo"
imageversion="0.0.1"

docker build -t ${imagename}:${imageversion} .