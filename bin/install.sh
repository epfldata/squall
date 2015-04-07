#!/bin/bash

#copy appropriate version of storm.yaml on local machine
mkdir -p ~/.storm
cp ../resources/storm.yaml ~/.storm/storm.yaml
./adjust_storm_yaml_locally.sh
