#!/bin/bash

usage() {
	echo "Usage: ./LatencyEstimator.sh <FOLDER>"
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

# DO NOT MODIFY THOSE VARIABLES!
TOTAL_LATENCY=0.0
declare -i COUNT
COUNT=0
FILES_WITH_LATENCIES=()
FILE_LATENCY=()

# Process files in folder one a time and update total latency
for FILE in `ls $FOLDER`
do
	LAST_LATENCY=`cat $FOLDER/$FILE | grep AVERAGE | tail -n 1 | cut -d' ' -f11`
	if [ "$LAST_LATENCY" != "" ]; then
		FILES_WITH_LATENCIES[$COUNT]=$FILE
		FILE_LATENCY[$COUNT]=$LAST_LATENCY
		TOTAL_LATENCY=`echo "scale=10; ${TOTAL_LATENCY} + ${LAST_LATENCY}" | bc`
		COUNT+=1
	fi
done

# Calculate and print total latency from all files
if [ $COUNT -eq 0 ]; then
	echo "No latency measurement located in folder $FOLDER."
	echo "No average will be reported"
	exit
else
	echo ""
	TOTAL_AVERAGE_LATENCY=`echo "scale=10; ${TOTAL_LATENCY}/${COUNT}" | bc`
	echo "Total average latency of ${COUNT} files is ${TOTAL_AVERAGE_LATENCY} msec"
fi

# Calculate and print latencies per file
echo ""
COUNT=0
for FILE in ${FILES_WITH_LATENCIES[@]}
do
	echo "Latency of file $FILE is ${FILE_LATENCY[$COUNT]} msec"
	COUNT+=1
done
echo ""
