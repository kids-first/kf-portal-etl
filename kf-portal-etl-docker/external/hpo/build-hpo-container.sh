#!/bin/bash

imagename="kids-first/hpo"
imageversion="0.0.1"

sudo docker build -t ${imagename}:${imageversion} .