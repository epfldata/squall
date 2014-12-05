#!/bin/bash

function usage() {
	echo "Usage: ./all_latency_estimator.sh <FOLDER>"
	exit
}

# Check correct number of command line arguments
if [ $# -ne 1 ]; then
	echo "Error: Illegal number of command line arguments. Required 1 argument and got $#. Exiting..."
	usage
fi

# Check correctness of provided folder
FOLDER=$1
if [ ! -d $FOLDER ]; then
	echo "Provided argument $FOLDER is not a folder (or folder doesn't exist). Exiting..."
	usage
fi


SUBDIRS=( `ls ${FOLDER}/` )
ALL_LAT_FILE=all_latencies.txt
ALL_EXC_FILE=all_exceptions.txt

#cleaning up old content
echo "" > $FOLDER/$ALL_LAT_FILE
echo "" > $FOLDER/$ALL_EXC_FILE

for SUBDIR in ${SUBDIRS[@]} 
do
	CUR_DIR=$FOLDER/$SUBDIR
	if [ -d $CUR_DIR ] && [ $SUBDIR != "cluster" ]; then
	  	echo "NEW CONFIGURATION: Latencies for $SUBDIR :" >> $FOLDER/$ALL_LAT_FILE
		./latency_estimator.sh $CUR_DIR >> $FOLDER/$ALL_LAT_FILE

		echo "NEW CONFIGURATION: Exceptions for $SUBDIR :" >> $FOLDER/$ALL_EXC_FILE
		./exception_locator.sh $CUR_DIR >> $FOLDER/$ALL_EXC_FILE
	fi
done
