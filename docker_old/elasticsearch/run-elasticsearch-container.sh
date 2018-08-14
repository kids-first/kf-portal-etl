#!/bin/bash
name=kf-es
sudo docker run --hostname $name --name $name -d -e "discovery.type=single-node" -e "cluster.name=kf-es" docker.elastic.co/elasticsearch/elasticsearch:6.2.4
