#!/bin/bash

cd ../

#../bin/lein clean
#../bin/lein deps

# TODO: use https://github.com/sbt/sbt-onejar to generate a single jar file
sbt clean package
