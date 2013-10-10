#!/bin/bash

./lein plugin install lein-localrepo "0.3"
./lein localrepo install ../contrib/trove-3.0.2.jar trove 3.0.2
./lein localrepo install ../contrib/bheaven-0.0.3.jar bheaven 0.0.3
./lein localrepo install ../contrib/je-5.0.84.jar bdb-je 5.0.84

#copy appropriate version of storm.yaml on local machine
mkdir -p ~/.storm
cp ../resources/storm.yaml ~/.storm/storm.yaml
./adjust_storm_yaml_locally.sh

mkdir -p /tmp/ramdisk
