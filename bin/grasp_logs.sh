#!/bin/bash
. ./storm_env.sh

function usage() {
	echo "Usage:      ./grasp_logs.sh <LOGS_LOCAL_PATH> <REMOVE_FROM_CLUSTER>"
	echo "               REMOVE_FROM_CLUSTER: YES/NO (to remove the log files from the cluster after the download)"
	exit
}


# Check correct number of command line arguments
if [ $# -ne 2 ]; then
	echo "Error: Illegal number of command line arguments. Required 2 argument and got $#. Exiting..."
	usage
fi

removeIfEmpty(){
	DIR=$1
	if [ `ls -A $DIR | wc -l` == 0 ]
	then 
		rm -r $DIR
	fi
}

# local path variables
# NOTE: For cget to work with directories, do not use / at the end of the path (although it still complains to an error)
LOGS_LOCAL_PATH=$1
REMOVE_FROM_CLUSTER=$2
CLUSTER_GATHER_DIR=/localhome/datalab/avitorovic/gather_logs
MASTER_LOG_DIR=$CLUSTER_GATHER_DIR/master
ZOO_FILEPATH=$ZOOKEEPERPATH/consoleZoo.txt

mkdir -p $LOGS_LOCAL_PATH
: ${LOGS_LOCAL_PATH:?"Need to set LOGS_LOCAL_PATH non-empty"}
mkdir -p $LOGS_LOCAL_PATH
rm -rf $LOGS_LOCAL_PATH/*
ssh $MASTER mkdir -p $CLUSTER_GATHER_DIR
ssh $MASTER mkdir -p $MASTER_LOG_DIR

# All of the following goes to the $CLUSTER_GATHER_DIR (or its subdirectories)
# collect zookeeper log
ssh $MASTER cget $ZOO_FILEPATH $CLUSTER_GATHER_DIR
# collect master logs
ssh $MASTER cp -r $STORM_LOGPATH/* $MASTER_LOG_DIR
# collect logs from blades
ssh $MASTER cget $STORM_LOGPATH $CLUSTER_GATHER_DIR

# Copy $CLUSTER_GATHER_DIR to local machine
scp -r $MASTER:$CLUSTER_GATHER_DIR/* $LOGS_LOCAL_PATH
# delete unused blade machine logs on the local machine
for FULLDIR in $LOGS_LOCAL_PATH/*; do
	if [ -d $FULLDIR ]; then
		removeIfEmpty "$FULLDIR"
	fi
done

# Cleaning Master/Blades
if [ $REMOVE_FROM_CLUSTER == "YES" ]; then
  ./delete_logs.sh
fi
# Delete Master:CLUSTER_GATHER_DIR
: ${CLUSTER_GATHER_DIR:?"Need to set CLUSTER_GATHER_DIR non-empty"}
ssh $MASTER rm -r $CLUSTER_GATHER_DIR/*
