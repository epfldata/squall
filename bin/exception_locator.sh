#!/bin/bash

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
for FILE in `ls $FOLDER`
do
	EXCEPTIONS=`cat $FOLDER/$FILE | grep "Exception" | cut -d':' -f2  | sort | uniq`
	if [ "$EXCEPTIONS" != "" ]; then
		echo "$FILE reported the following exceptions:"
		echo -e "\t$EXCEPTIONS"
	fi
done
echo ""
