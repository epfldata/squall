#!/bin/bash

#Args begin
MACHINE=$1
PORT=$2
REMOTE_FOLDER=$3
LOCAL_FOLDER=$4

#For example,
#MACHINE=squalldata@icdatasrv2
#PORT=22
#REMOTE_FOLDER=/data/squall_zone/profiling/output
#LOCAL_FOLDER=snapshots
#Args end

### Methods begin
get_file_serial() {
	# first get rid of extension, and then take the 5th part when spliting with '-'
	# filename(arg 1) is in the form: worker-2013-03-21-332.snapshot
	declare -i SERIAL
	SERIAL=`echo ${1%%.*} | cut -d'-' -f5`
	return $SERIAL
}
### Methods end

# get a list
ALL_FILES=`ssh -p $PORT $MACHINE 'ls '$REMOTE_FOLDER`
#echo $ALL_FILES

# find the size of the list
declare -i NUM_FILES
NUM_FILES=0
for x in $ALL_FILES
do
    NUM_FILES+=1
done
#echo $NUM_FILES

# If there are more than 1 file inside, then we have to do download something
if [ $NUM_FILES -gt 0 ]; then
	declare -i MAX_SERIAL
	MAX_SERIAL=-1
	
	declare -i SERIAL
	for file in ${ALL_FILES} ; do
		filename=${file##*/}
		#echo $filename
		get_file_serial $filename
		SERIAL=$?
		#echo "SERIAL is $SERIAL"
		if [ $MAX_SERIAL -lt $SERIAL ]; then
			MAX_SERIAL=$SERIAL
			MAX_FILE=$filename
			#echo "MAX_FILE is $MAX_FILE"
		fi
	done
	echo "File with max index is $MAX_FILE"
	scp -P $PORT $MACHINE:$REMOTE_FOLDER/$MAX_FILE $LOCAL_FOLDER
fi
