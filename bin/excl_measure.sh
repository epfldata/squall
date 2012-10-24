#!/bin/bash

. ./storm_version.sh

KILL_TIME=35
WAIT_STAT=300

if [ $# -ne 1 ]
then
	echo "Should provide a file. Exiting..."
	exit
fi

if [ ! -f $1 ]; then
	echo "$1 is not a file. Exiting..."
	exit
fi

EXCL_FILE=$1
STARTER_DIR=../$STORMNAME/storm-starter
STORM_BIN=../$STORMNAME/bin
CURR_DIR=`pwd`

#compiling
echo "Compiling $EXCL_FILE ..."
cp $EXCL_FILE $STARTER_DIR/src/jvm/storm/starter/ExclamationTopology.java
cd $STARTER_DIR
lein uberjar
cd $CURR_DIR

#running new code
echo "Running storm_starter Exclamation $EXCL_FILE topology ..."
$STORM_BIN/storm jar $STARTER_DIR/storm-starter-0.0.1-SNAPSHOT-standalone.jar storm.starter.ExclamationTopology Exclamation

#grasping statistics
sleep $WAIT_STAT
echo "Grasping statistics for $EXCL_FILE ..."
./get_topology_stats.sh > ${EXCL_FILE}.statistics

#killing the topology
echo "Killing the topology $EXCL_FILE ..."
$STORM_BIN/storm kill Exclamation
sleep $KILL_TIME
echo "Topology killed $EXCL_FILE ."
