#!/bin/bash

. ./storm_env.sh

# The following is needed only if we use DBToaster operators
# On Linux machines, we need to install Scala (at least 2.10)
# wget http://www.scala-lang.org/files/archive/scala-2.10.4.deb
# sudo dpkg -i scala-2.10.4.deb
# sudo apt-get update
# sudo apt-get install scala

# Downloading Storm and putting it to the right place
echo "Downloading and extracting $STORMNAME ..."
wget http://mirror.easyname.ch/apache/storm/$STORMNAME/$STORMNAME.tar.gz
tar -xzf $STORMNAME.tar.gz
mv $STORMNAME ..
rm $STORMNAME.tar.gz

# Compiling Squall and generating dependencies 
echo "Compiling Squall and generating dependencies ..."
CURR_DIR=`pwd`
cd ..
sbt package
sbt assemblyPackageDependency
cd $CURR_DIR
# The following is used only for the Cluster Mode
cp ../squall-core/target/squall-dependencies-0.2.0.jar ../$STORMNAME/lib/
