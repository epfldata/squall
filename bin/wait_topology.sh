#!/bin/bash

. ./storm_version.sh

USERNAME=username
TOPOLOGY_NAME=${USERNAME}_$1

STORM_PATH=../$STORMNAME/
STORM_LIB_PATH=$STORM_PATH/lib

if [ $# -ne 1 ] 
then
  echo "Missing TOPOLOGY_NAME. Exiting..."
  exit
fi

cd wait_topology
./compile.sh
cd ..

echo "Waiting for topology $TOPOLOGY_NAME to finish..."
java -cp $STORM_PATH/$STORMNAME.jar:$STORM_LIB_PATH/libthrift7-0.7.0.jar:$STORM_LIB_PATH/log4j-1.2.16.jar:$STORM_LIB_PATH/slf4j-api-1.5.8.jar:$STORM_LIB_PATH/slf4j-log4j12-1.5.8.jar:wait_topology/. topologydone.Main $TOPOLOGY_NAME