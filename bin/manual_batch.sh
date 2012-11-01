#!/bin/bash

. ./storm_version.sh

KILL_TIME=30

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

BACKUP_DIR=~/backup_squall
SQUALL_SRC=src
SQUALL_SRC_DIR=../squall/$SQUALL_SRC

CONFIG_PATH=$1
CONFIG_FILE=${CONFIG_PATH##*/}
OUTPUT_DIR=$2/$CONFIG_FILE
OUTPUT_DIR=${OUTPUT_DIR%%.*}
CODE_DIR=$2/../code
STORM_BIN=../$STORMNAME/bin
CURR_DIR=`pwd`

#saving original code
rm -rf $BACKUP_DIR
mkdir -p $BACKUP_DIR
cp -r $SQUALL_SRC_DIR $BACKUP_DIR

#compiling
cd $CODE_DIR
./copier.sh
cd $CURR_DIR
./recompile.sh

#returning original code
rm -rf $SQUALL_SRC_DIR
cp -r $BACKUP_DIR/$SQUALL_SRC $SQUALL_SRC_DIR

#running new code
echo "Running Squall with config $CONFIG_PATH topology ..."
./squall_plan_runner_cluster.sh $CONFIG_PATH

#grasping statistics
mkdir -p $OUTPUT_DIR
echo "Grasping statistics for $CONFIG_PATH ..."
 ./get_topology_stats.sh > $OUTPUT_DIR/StormLike.statistics
#TODO: This could not grasp any statistics after topology is killed

#grasping output of our MACE-like timestamp mechanism, and removing it for not spoiling further results
for blade in {1..10}
do
  for zone in {1001..1022}
  do
    scp -P $zone squalldata@icdatasrv${blade}:/data/squall_zone/logs/worker* $OUTPUT_DIR/worker-${blade}-${zone}
    ssh -p $zone squalldata@icdatasrv${blade} 'cd /data/squall_zone/logs; rm -rf worker*'
  done
done

# just to make sure that topology is removed from UI
sleep $KILL_TIME
