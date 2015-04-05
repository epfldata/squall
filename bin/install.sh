#!/bin/bash

# workaround fora github bug (see https://www.drupal.org/node/2304983)
mkdir -p $HOME/.lein/self-installs
cp ../contrib/leiningen-1.7.1-standalone.jar $HOME/.lein/self-installs/
./lein plugin install lein-localrepo "0.3"
./lein localrepo install ../contrib/trove-3.0.2.jar trove 3.0.2
./lein localrepo install ../contrib/bheaven-0.0.3.jar bheaven 0.0.3
./lein localrepo install ../contrib/je-5.0.84.jar bdb-je 5.0.84
./lein localrepo install ../contrib/ujmp-complete-0.2.5.jar ujmp 0.2.5
./lein localrepo install ../contrib/opencsv-2.3.jar opencsv 2.3

#copy appropriate version of storm.yaml on local machine
mkdir -p ~/.storm
cp ../resources/storm.yaml ~/.storm/storm.yaml
./adjust_storm_yaml_locally.sh
