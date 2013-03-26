#!/bin/bash

. ./storm_version.sh

MACHINE=squalldata@icdatasrv5
LOG_FILE=/data/squall_zone/logs/nimbus.log

TOPOLOGY_NAME_PREFIX=username
TOPOLOGY_NAME=${TOPOLOGY_NAME_PREFIX}_$1

STORM_PATH=../$STORMNAME/
STORM_LIB_PATH=$STORM_PATH/lib
EXEC_DIR=wait_topology

WAIT_SUBMIT=3
TIMEOUT_INVOKE=1

if [ $# -ne 1 ] 
then
  echo "Missing TOPOLOGY_NAME. Exiting..."
  exit
fi

# return 1 indicates that there was an error
# arg $1 is $TOPOLOGY_NAME
checkErrors(){	
	# Reassigning username_10G_tpch7
	msg='"Reassigning '$1'" '
	# echo $msg
	reassign=`ssh $MACHINE 'grep '$msg $LOG_FILE`
	#echo $reassign
	if [ -z "$reassign" ]; then
		return 0
	else
		return 1
	fi
}

cd $EXEC_DIR
./compile.sh
cd ..

echo "STATUS: Waiting for topology $TOPOLOGY_NAME to finish..."
sleep $WAIT_SUBMIT
FIRST_TIME=true
ALREADY_KILLED=false
while true
do
	status=`java -cp $STORM_PATH/$STORMNAME.jar:$STORM_LIB_PATH/libthrift7-0.7.0.jar:$STORM_LIB_PATH/log4j-1.2.16.jar:$STORM_LIB_PATH/slf4j-api-1.5.8.jar:$STORM_LIB_PATH/slf4j-log4j12-1.5.8.jar:$EXEC_DIR/. topologydone.Main $TOPOLOGY_NAME`
	if [ $status == "FINISHED" ]; then
		if [ $FIRST_TIME == "true" ]; then
			echo "... Most probably topology $TOPOLOGY_NAME did not even start."
		fi
		break
	fi
	if [ $ALREADY_KILLED == "false" ]; then
		checkErrors $TOPOLOGY_NAME
		if [ "$?" == "1" ]; then
			echo "ERROR: Reassigning happened in topology $TOPOLOGY_NAME. The topology will be killed..."
			storm kill $TOPOLOGY_NAME
			ALREADY_KILLED=true
		fi
	fi
	FIRST_TIME=false
	sleep $TIMEOUT_INVOKE
done
