#!/bin/bash

root_dir=$1
github_account=$2
github_repo=$3
github_tag=$4
build_cmd=$5

home_dir=$root_dir/$github_repo
download_url="https://github.com/$github_account/$github_repo/archive/$github_tag.zip"
echo "Downloading and Installing '$github_repo' with tag '$github_tag'"
wget $download_url
unzip $github_tag.zip
mv $home_dir-$github_tag $home_dir
rm -f $github_tag.zip

echo " "
echo "Building '$github_repo' with tag '$github_tag' with command '$build_cmd'"
cd $home_dir
$build_cmd

