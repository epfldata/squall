#!/bin/bash

cd ../compilation

# these commands have to be executed only once (but they are idempotent, no harm executing it multiple times)
mvn install:install-file -DgroupId=jsqlparser -DartifactId=jsqlparser -Dversion=0.7.0 -Dpackaging=jar -Dfile=../lib/jsqlparser-0.7.0.jar
../bin/lein clean
../bin/lein deps

# this has to be executed every time
../bin/lein uberjar