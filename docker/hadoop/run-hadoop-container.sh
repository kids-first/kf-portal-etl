#!/bin/bash
name=kf-hadoop
sudo docker run --hostname name --name $name -d sequenceiq/hadoop-docker:2.7.0