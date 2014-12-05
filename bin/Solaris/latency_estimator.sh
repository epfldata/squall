#!/bin/bash
# Extract information from worker.log files when printing latency is turned on (It works with no Ackers)

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
declare -i COUNT_TUPLES
COUNT_TUPLES=0
declare -i TOTAL_COUNT_TUPLES
TOTAL_COUNT_TUPLES=0

FILES_WITH_LATENCIES=()
FILE_LATENCY=()

# Print execution time
echo ""
TIME=`find $FOLDER | xargs grep "Job Elapsed Time" | cut -d':' -f3- | sed 's/ //g'`
HOURS=`echo $TIME | cut -d':' -f1`
MINUTES=`echo $TIME | cut -d':' -f2`
SECONDS=`echo $TIME | cut -d':' -f3`
TOTAL_TIME_SECONDS=`echo "($HOURS * 3600) + ($MINUTES * 60) + ($SECONDS)" | bc`
echo "Total execution time is $TOTAL_TIME_SECONDS seconds"

# Process files in folder one a time and update total latency
for FILEPATH in `find $FOLDER -name \*.log`
do
	LAST_LATENCY=`cat $FILEPATH | grep AVERAGE | tail -n 1 | cut -d' ' -f11 | sed 's/ms.//g'`
	if [ "$LAST_LATENCY" != "" ]; then
		# we have to weight latency according to the number of tuples outputted
		COUNT_TUPLES=`cat $FILEPATH | grep "Sent Tuples" | tail -n 1 | rev | cut -d',' -f1 | rev`
		TOTAL_COUNT_TUPLES=`echo "scale=10; ${TOTAL_COUNT_TUPLES} + ${COUNT_TUPLES}" | bc`
	
		# the following command extracts dirname/filename from /dir/dir/dir/dir/dir/dirname/filename
		FILEID=$(echo "${FILEPATH}" | rev | cut -d'/' -f1,2 | rev)
		FILES_WITH_LATENCIES[$COUNT]=$FILEID
		FILE_LATENCY[$COUNT]=$LAST_LATENCY
		TOTAL_LATENCY=`echo "scale=10; ${TOTAL_LATENCY} + (${LAST_LATENCY} * ${COUNT_TUPLES})" | bc`
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
	TOTAL_AVERAGE_LATENCY=`echo "scale=10; ${TOTAL_LATENCY}/${TOTAL_COUNT_TUPLES}" | bc`
	echo "Total average latency of ${COUNT} files is ${TOTAL_AVERAGE_LATENCY} msec"
fi

# Calculate and print latencies per file
echo ""
COUNT=0
for SUPERVISOR in ${FILES_WITH_LATENCIES[@]}
do
	echo "Latency of file $SUPERVISOR is ${FILE_LATENCY[$COUNT]} msec"
	COUNT+=1
done
echo ""
