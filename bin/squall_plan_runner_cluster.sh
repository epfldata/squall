#!/bin/bash
BIN_PATH=../bin
if [ ! -d $BIN_PATH ]; then
	echo "Bin directory '$BIN_PATH' does not exit. Exiting..."
	exit
fi

./recompile.sh
. ./storm_version.sh

# Set whether you want profiling information gathered
declare -i PROFILE_ENABLED
PROFILE_ENABLED=0
SNAPSHOT_DIR=../squallSnapshots #Required for profiling
STORM_OUTPUT=$SNAPSHOT_DIR/stormOutput

# Initialize profiling
if [ $PROFILE_ENABLED -eq 1 ]; then
	# Check if snapshot_dir exists (if profiling is enabled)
	if [ ! -d $SNAPSHOT_DIR ]; then
		echo "Snapshot directory '$SNAPSHOT_DIR' does not exist. Exiting..."
		exit
	fi
	# Remove old output
	rm -rf $STORM_OUTPUT/*
	rm -rf $SNAPSHOT_DIR/*
	# Setup cluster for profiling (if cluster is already running in profiling mode, this is NOOP
	./profiling.sh START
fi

CONFIG_DIR=../test/squall_plan_runner/confs/cluster

# If no configuration file is given, use default
if [ $# -ne 1 ]
then
	CONFIG_PATH=$CONFIG_DIR/1G_hyracks_parallel
else
	CONFIG_PATH=$1
fi

# check if your configuration file exists
if ! [ -f $CONFIG_PATH ];
then
   echo "File $CONFIG_PATH does not exist! Please specify a valid configuration file!"
   exit
fi

TIME_BEFORE="$(date +%s)"
../$STORMNAME/bin/storm jar ../deploy/squall-2.0-standalone.jar plan_runner.main.Main $CONFIG_PATH
./waitTopology.sh
TIME_AFTER="$(date +%s)"
ELAPSED_TIME="$(expr $TIME_AFTER - $TIME_BEFORE)"
echo | awk -v D=$ELAPSED_TIME '{printf "Job Elapsed Time: %02d:%02d:%02d\n",D/(60*60),D%(60*60)/60,D%60}'

# Gather the profiling information
if [ $PROFILE_ENABLED -eq 1 ]; then
	echo "Gathering profiling information... "
        mkdir -p $SNAPSHOT_DIR/$confname
        cd $SNAPSHOT_DIR/$confname
        ${BIN_PATH}/graspSnapshots.sh
        ${BIN_PATH}/deleteSnapshots.sh        
	${BIN_PATH}/graspOutput.sh
	cd $STORM_OUTPUT
	rake
fi
