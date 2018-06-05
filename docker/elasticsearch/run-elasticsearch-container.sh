#!/bin/bash

sudo docker run --name kf-es --network host -d -e "discovery.type=single-node" -e "cluster.name=kf-es" docker.elastic.co/elasticsearch/elasticsearch:6.2.4
