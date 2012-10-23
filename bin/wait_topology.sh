#!/bin/bash

. ./storm_version.sh

TOPOLOGY_NAME_PREFIX=username
TOPOLOGY_NAME=${TOPOLOGY_NAME_PREFIX}_$1

STORM_PATH=../$STORMNAME/
STORM_LIB_PATH=$STORM_PATH/lib
EXEC_DIR=wait_topology

if [ $# -ne 1 ] 
then
  echo "Missing TOPOLOGY_NAME. Exiting..."
  exit
fi

cd $EXEC_DIR
./compile.sh
cd ..

echo "STATUS: Waiting for topology $TOPOLOGY_NAME to finish..."
java -cp $STORM_PATH/$STORMNAME.jar:$STORM_LIB_PATH/libthrift7-0.7.0.jar:$STORM_LIB_PATH/log4j-1.2.16.jar:$STORM_LIB_PATH/slf4j-api-1.5.8.jar:$STORM_LIB_PATH/slf4j-log4j12-1.5.8.jar:$EXEC_DIR/. topologydone.Main $TOPOLOGY_NAME