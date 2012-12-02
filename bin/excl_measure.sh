#!/bin/bash

. ./storm_version.sh

KILL_TIME=35
WAIT_STAT=180

if [ $# -ne 2 ]
then
	echo "Should provide an input file and output directory. Exiting..."
	exit
fi

if [ ! -f $1 ]; then
	echo "$1 is not a file. Exiting..."
	exit
fi

if [ ! -d $2 ]; then
	echo "$2 is not a directory. Exiting..."
	exit
fi

EXCL_FILE=$1
OUTPUT_DIR=$2/${EXCL_FILE##*/}
OUTPUT_DIR=${OUTPUT_DIR%%.*}
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
mkdir -p $OUTPUT_DIR
echo "Grasping statistics for $EXCL_FILE ..."
 ./get_topology_stats.sh > $OUTPUT_DIR/StormLike.statistics

#killing the topology
echo "Killing the topology $EXCL_FILE ..."
$STORM_BIN/storm kill Exclamation
sleep $KILL_TIME
echo "Topology killed $EXCL_FILE ."

#grasping output of our MACE-like timestamp mechanism, and removing it for not spoiling further results
for blade in {1..10}
do
  for zone in {1001..1022}
  do
    scp -P $zone squalldata@icdatasrv${blade}:/data/squall_zone/logs/worker* $OUTPUT_DIR/worker-${blade}-${zone}
    ssh -p $zone squalldata@icdatasrv${blade} 'cd /data/squall_zone/logs; rm -rf worker*'
  done
done
