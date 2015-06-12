#!/bin/bash
. ./storm_env.sh

function usage() {
	echo "Usage:      ./grasp_snapshots.sh <LOCAL_PATH>"
	exit
}


# Check correct number of command line arguments
if [ $# -ne 1 ]; then
	echo "Error: Illegal number of command line arguments. Required 1 argument and got $#. Exiting..."
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
SNAPSHOT_LOCAL_PATH=$1
MASTER_SNAPSHOT_DIR=$CLUSTER_GATHER_SNAPSHOT_DIR/master

mkdir -p $SNAPSHOT_LOCAL_PATH
: ${SNAPSHOT_LOCAL_PATH:?"Need to set SNAPSHOT_LOCAL_PATH non-empty"}
mkdir -p $SNAPSHOT_LOCAL_PATH
rm -rf $SNAPSHOT_LOCAL_PATH/*
ssh $MASTER mkdir -p $CLUSTER_GATHER_SNAPSHOT_DIR
ssh $MASTER mkdir -p $MASTER_SNAPSHOT_DIR

# All of the following goes to the $CLUSTER_GATHER_SNAPSHOT_DIR (or its subdirectories)
# collect master snapshots
ssh $MASTER cp -r $STORM_SNAPSHOTPATH/* $MASTER_SNAPSHOT_DIR
# collect snapshots from blades
ssh $MASTER cget $STORM_SNAPSHOTPATH $CLUSTER_GATHER_SNAPSHOT_DIR

# Copy $CLUSTER_GATHER_SNAPSHOT_DIR to local machine
scp -r $MASTER:$CLUSTER_GATHER_SNAPSHOT_DIR/* $SNAPSHOT_LOCAL_PATH
# delete unused blade machine snapshots on the local machine
for FULLDIR in $SNAPSHOT_LOCAL_PATH/*; do
	if [ -d $FULLDIR ]; then
		removeIfEmpty "$FULLDIR"
	fi
done

# Delete Master:CLUSTER_GATHER_SNAPSHOT_DIR
: ${CLUSTER_GATHER_SNAPSHOT_DIR:?"Need to set CLUSTER_GATHER_SNAPSHOT_DIR non-empty"}
ssh $MASTER rm -r $CLUSTER_GATHER_SNAPSHOT_DIR/*
