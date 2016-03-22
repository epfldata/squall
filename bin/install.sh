#!/bin/bash

#copy appropriate version of storm.yaml on local machine
mkdir -p ~/.storm
cp ../resources/storm.yaml ~/.storm/storm.yaml
./adjust_storm_yaml_locally.sh

# On Linux machines, we need to install Scala (at least 2.10)
# wget http://www.scala-lang.org/files/archive/scala-2.10.4.deb
# sudo dpkg -i scala-2.10.4.deb
# sudo apt-get update
# sudo apt-get install scala
