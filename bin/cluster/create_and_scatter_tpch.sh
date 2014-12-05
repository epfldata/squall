#!/bin/bash

# This script is meant to be executed on Master

function usage() {
	echo "Usage:      ./create_and_scatter_tpch.sh <PATH> <SCALING_FACTOR> <SKEW>"
	echo "               PATH is relative to /data/lab/icdatasrv_saved/squall_blade/data/ on master and /data/lab/squall_data/ on blades."
	echo "               SCALING_FACTOR represents the size in GBs."
	echo "               SKEW goes from 0 to 4 (0 means no skew, 4 means max skew)."
	exit
}

# Check correct number of command line arguments
if [ $# -ne 3 ]; then
	echo "Error: Illegal number of command line arguments. Required 3 argument and got $#. Exiting..."
	usage
fi

# Example DB=tpchdb/Z1/10G
DB=$1
# Example SF=1 (means 1GB)
SF=$2
# Example SKEW=1 (means Z1)
Z=$3
MASTER_DATA_DIR=/data/lab/icdatasrv_saved/squall_blade/data/$DB
BLADES_DATA_DIR=/data/lab/squall_data/$DB/

# create dirs on the cluster, if they don't already exist
mkdir -p $MASTER_DATA_DIR
cexec mkdir -p $BLADES_DATA_DIR

# Create output on master
./dbgen -s $SF -z $Z
mv order.tbl orders.tbl
mv *.tbl $MASTER_DATA_DIR

# Copy it on Blades
echo "Datasets produced. Copying it..."
cpush $MASTER_DATA_DIR/* $BLADES_DATA_DIR
