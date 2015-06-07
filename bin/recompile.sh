#!/bin/bash

cd ../

#sbt clean

# Generate squall-0.2.0.jar
sbt package

# Generate squall-standalone-0.2.0.jar
#sbt assembly

# Generate squall-dependencies-0.2.0.jar
#sbt assemblyPackageDependency
