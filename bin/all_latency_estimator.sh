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
LAT_FILE=latency.txt
EXC_FILE=exceptions.txt
ALL_LAT_FILE=all_latencies.txt
ALL_EXC_FILE=all_exceptions.txt

for DIR in ${SUBDIRS[@]} 
do
  echo "NEW CONFIGURATION: Latencies for $DIR :" > $FOLDER/$DIR/$LAT_FILE
  ./latency_estimator.sh $FOLDER/$DIR >> $FOLDER/$DIR/$LAT_FILE
  cat $FOLDER/$DIR/$LAT_FILE >> $FOLDER/$ALL_LAT_FILE

  echo "NEW CONFIGURATION: Exceptions for $DIR :" > $FOLDER/$DIR/$EXC_FILE
  ./exception_locator.sh $FOLDER/$DIR >> $FOLDER/$DIR/$EXC_FILE
  cat $FOLDER/$DIR/$EXC_FILE >> $FOLDER/$ALL_EXC_FILE
done
