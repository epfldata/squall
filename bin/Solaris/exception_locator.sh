#!/bin/bash
# From STORM_DATA_DIR (which contains all the Storm log files), it extracts the Exceptions

function usage() {
	echo "Usage: ./exception_locator.sh <FOLDER>"
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

# Process files in folder one a time and update total latency
echo ""
for FILEPATH in `find $FOLDER -name \*.log`
do
#	EXCEPTIONS=`cat $FILEPATH | grep "Exception" | cut -d':' -f2  | sort | uniq`
	EXCEPTIONS=`cat $FILEPATH | grep "Exception" `
	if [ "$EXCEPTIONS" != "" ]; then
		echo "$FILEPATH reported the following exceptions:"
		echo -e "\t$EXCEPTIONS"
	fi
	WARNINGLAT=`cat $FILEPATH | grep "WARNINGLAT" `
	if [ "$WARNINGLAT" != "" ]; then
		echo "$FILEPATH reported the following exceptions:"
		echo -e "\t$WARNINGLAT"
	fi
done
echo ""
