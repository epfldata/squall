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
	echo "Error: Illegal number of command line arguments. Required 4 argument and got $#. Exiting..."
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
RAKE_PATH=../bin/rake_files/
SNAPSHOT_DIR_NAME=snapshots
STORM_LOG_DIR_NAME=logs
EXEC_LOG_FILE_NAME=local_exec.info

# 1. deleting old data and recompile
echo "Recompiling ..."
./recompile.sh

# 2. storm.yaml is set at the beginning and it is not returned to its original state
echo "Changing the configuration and reseting... "
if [ $PROFILING == YES ] 
then
  ./profiling.sh START $RESTART_ANYWAY
else
  ./profiling.sh END $RESTART_ANYWAY
fi

# 3. for each generated file, run it and wait until it is terminated
TESTCONFS=( `ls ${CONF_PATH}/` )
COUNT=${#TESTCONFS[@]}
declare -i i
i=1
for config in ${CONF_PATH}* ; do
	echo "Running config file $i ($config) out of ${COUNT}..."

	# 1. running a topology
	confname=${config##*/}
        OUTPUT_PATH=$BASE_PATH/$confname/
        mkdir -p $OUTPUT_PATH
        rm -rf ${OUTPUT_PATH}*
	./squall_cluster.sh $MODE $CONF_PATH/$confname > $OUTPUT_PATH/$EXEC_LOG_FILE_NAME
	#waiting for topology to finish is now in squall_cluster.sh
	# incrementing the counter

	# 2. grasping profiling info, if any
        if [ $PROFILING == YES ]
  	then
          echo "Downloading profiling information for config file $i ($config) out of ${COUNT}..."
	  #getting snapshots
  	  SNAPSHOT_PATH=$OUTPUT_PATH/$SNAPSHOT_DIR_NAME
          mkdir -p $SNAPSHOT_PATH
          ./grasp_snapshots.sh $SNAPSHOT_PATH

          #deleting snapshots from cluster, so that the following snapshots are not spoiled
          ./delete_snapshots.sh
	fi	

	# 3. grasping and deleting storm logs
        echo "Downloading storm log information for config file $i ($config) out of ${COUNT}..."
	STORM_LOGS_PATH=$OUTPUT_PATH/$STORM_LOG_DIR_NAME/
	mkdir -p $STORM_LOGS_PATH
	rm -rf ${STORM_LOGS_PATH}*
	./grasp_logs.sh $STORM_LOGS_PATH YES

	# 4. Extracting timing information.
	echo "Extracting cluster_exec.info for config file $i ($config) out of ${COUNT}..."
	CURR_DIR=`pwd`
	cd $RAKE_PATH
	rake -f extract_time.rb extract_one[$MODE,$BASE_PATH,$confname,$STORM_LOGS_PATH]
	cd $CURR_DIR

        i+=1
done
