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

# Calculate throughput (may be wrong if topology name doesn't contain workload
# size run in this experiment)
TOPOLOGY_NAME=`find $FOLDER | xargs grep "Finished submitting topology" | cut -d':' -f3`
OLDIFS=$IFS
IFS="_"
read -a array <<< "$(printf "%s" "${TOPOLOGY_NAME}")"
IFS=$OLDIFS
WORKLOAD_SIZE=""
for token in ${array[@]}
do
	if [ "$token" == "0.01G" -o "$token" == "0.1G" -o "$token" == "1G" -o "$token" == "10G" -o "$token" == "50G" -o "$token" == "100G" ]; then
		WORKLOAD_SIZE=$token
		break;
	fi
done
# Print throughput
if [ "$WORKLOAD_SIZE" != "" ]; then
	echo "Topology size is $WORKLOAD_SIZE (extracted from topology name)"
	WORKLOAD_SIZE=`echo $WORKLOAD_SIZE | sed 's/G//g'`
	THROUGHPUT=`echo "scale=2; ${WORKLOAD_SIZE}*1024 / $TOTAL_TIME_SECONDS" | bc`
	echo "Throughput is $THROUGHPUT MB/sec"
else
	echo "Couldn't locate workload size in topology name. You have to calculate throughput manually."
fi

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

# Print predicted latency
PREDICTED_LATENCY=`find $FOLDER | xargs grep "Predicted" | cut -d' ' -f14`
echo "Predicted latency is $PREDICTED_LATENCY msec"

# Calculate and print latencies per file
echo ""
COUNT=0
for FILE in ${FILES_WITH_LATENCIES[@]}
do
	echo "Latency of file $FILE is ${FILE_LATENCY[$COUNT]} msec"
	COUNT+=1
done
echo ""
