#!/bin/bash

function usage() {
	echo "Usage:      ./loop_squall_cluster.sh <MODE> <PROFILING> <RESTART_ANYWAY> <BASE_PATH>"
	echo "               MODE: PLAN_RUNNER or SQL "
	echo "               PROFILING: YES or NO "
	echo "               RESTART_ANYWAY: YES or NO (this is for cleaning storm .log files)"
	echo "               BASE_PATH: ../experiments/series_name"
	exit
}


# Check correct number of command line arguments
if [ $# -ne 4 ]; then
	echo "Error: Illegal number of command line arguments. Required 3 argument and got $#. Exiting..."
	usage
fi
MODE=$1
PROFILING=$2
RESTART_ANYWAY=$3
BASE_PATH=$4
# Check if arg3 is a directory
if [ ! -d $BASE_PATH ]; then
	echo "Provided argument $BASE_PATH is not a folder (or folder doesn't exist). Exiting..."
	usage
fi


CONF_PATH=$BASE_PATH/cluster/
STORM_DATA_PATH=$BASE_PATH/data/
RAKE_PATH=../bin/rake_files/
SNAPSHOT_DIR_NAME=snapshots
EXEC_LOG_FILE_NAME=local_exec.info

# 1. deleting old data and recompile
mkdir -p $STORM_DATA_PATH
rm -rf ${STORM_DATA_PATH}*
./recompile.sh

# 2. storm.yaml is set at the beginning and it is not returned to its original state
if [ $PROFILING == YES ] 
then
  ./profiling.sh START $RESTART_ANYWAY
else
  ./profiling.sh END $RESTART_ANYWAY
fi

# 3. for each generated file, run it and wait until it is terminated
for config in ${CONF_PATH}* ; do
        #wait until resources are freed from previous execution, the same as TOPOLOGY_MESSAGE_TIMEOUT_SECS
        #explained in Killing topology section in https://github.com/nathanmarz/storm/wiki/Running-topologies-on-a-production-cluster

	confname=${config##*/}
        OUTPUT_PATH=$BASE_PATH/$confname/
        mkdir -p $OUTPUT_PATH
        rm -rf ${OUTPUT_PATH}*
	./squall_cluster.sh $MODE $CONF_PATH/$confname > $OUTPUT_PATH/$EXEC_LOG_FILE_NAME

	#waiting for topology to finish is now in squall_cluster.sh
        if [ $PROFILING == YES ] 
  	then
	  #getting snapshots
  	  SNAPSHOT_PATH=$OUTPUT_PATH/$SNAPSHOT_DIR_NAME
          mkdir -p $SNAPSHOT_PATH
          ./grasp_snapshots.sh $SNAPSHOT_PATH

          #deleting snapshots from cluster, so that the following snapshots are not spoiled
          ./delete_snapshots.sh
	fi	
done

# 4. grasp output
./grasp_output.sh $STORM_DATA_PATH

# 5. Extracting timing information.
CURR_DIR=`pwd`
cd $RAKE_PATH
rake -f extract_time.rb extract[$MODE,$BASE_PATH,$CONF_PATH,$STORM_DATA_PATH]
cd $CURR_DIR
