#!/bin/bash

# This script is meant to be executed on Master

function usage() {
	echo "Usage:      ./create_and_scatter_jps.sh <PATH> <RELSIZE>"
	echo "               PATH is relative to /data/lab/icdatasrv_saved/squall_blade/data/ on master and /data/lab/squall_data/ on blades."
	echo "               RELSIZE is the number of tuples in one relation (both relations are of the same size)."
	exit
}

# Check correct number of command line arguments
if [ $# -ne 2 ]; then
	echo "Error: Illegal number of command line arguments. Required 2 argument and got $#. Exiting..."
	usage
fi

# Example DB=jps/wfo_0_46/12M
DB=$1
# Example RELSIZE=12000000
RELSIZE=$2
MASTER_DATA_DIR=/data/lab/icdatasrv_saved/squall_blade/data/$DB
BLADES_DATA_DIR=/data/lab/squall_data/$DB/

# create dirs on the cluster, if they don't already exist
mkdir -p $MASTER_DATA_DIR
cexec mkdir -p $BLADES_DATA_DIR

# Create output on master
./generate_synthetic_jps.py $MASTER_DATA_DIR/jps $RELSIZE

# Copy it on Blades
echo "Datasets produced. Copying it..."
cpush $MASTER_DATA_DIR/* $BLADES_DATA_DIR
