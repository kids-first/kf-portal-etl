#!/bin/bash

# please run "sudo su" first

echo vm.max_map_count=262144 >> /etc/sysctl.conf

sudo apt-get update

sudo apt-get install \
    apt-transport-https \
    ca-certificates \
    curl \
    software-properties-common

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

sudo apt-key fingerprint 0EBFCD88

sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"

sudo apt-get update

# if 'lsb_release -sr' returns 18 or higher, run the following to install docker. Or run 'sudo apt-get install docker-ce'
sudo apt-get install docker.io