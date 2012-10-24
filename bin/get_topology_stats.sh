#!/bin/bash

. ./storm_version.sh

STORM_PATH=../$STORMNAME/
STORM_LIB_PATH=$STORM_PATH/lib
EXEC_DIR=topology_stats

cd $EXEC_DIR
./compile.sh
cd ..

java -cp $STORM_PATH/$STORMNAME.jar:$STORM_LIB_PATH/libthrift7-0.7.0.jar:$STORM_LIB_PATH/log4j-1.2.16.jar:$STORM_LIB_PATH/slf4j-api-1.5.8.jar:$STORM_LIB_PATH/slf4j-log4j12-1.5.8.jar:$STORM_LIB_PATH/commons-lang-2.5.jar:$EXEC_DIR/. stats.TopologyStats