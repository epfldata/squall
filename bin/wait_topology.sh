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
PRINTED_FINAL_STATS=false

FREEZED_SECONDS=$(( 60 * 60 * 3))
FREEZED_INVOCATIONS=$((FREEZED_SECONDS/TIMEOUT_INVOKE))
declare -i invocations
invocations=0

while true
do
	status=`java -cp $STORM_PATH/$STORMNAME.jar:$STORM_LIB_PATH/libthrift7-0.7.0.jar:$STORM_LIB_PATH/log4j-1.2.16.jar:$STORM_LIB_PATH/slf4j-api-1.5.8.jar:$STORM_LIB_PATH/slf4j-log4j12-1.5.8.jar:$EXEC_DIR/. topologydone.Main $TOPOLOGY_NAME`
	if [ $status == "KILLED" ]; then
		if [ $PRINTED_FINAL_STATS == "false" ]; then
			echo "******************BEGIN OF FINAL TOPOLOGY_STATS******************"
			./get_topology_stats.sh
			echo "******************END OF FINAL TOPOLOGY_STATS******************"
			PRINTED_FINAL_STATS=true
		fi
	fi
	if [ $status == "FINISHED" ]; then
		if [ $FIRST_TIME == "true" ]; then
			echo "... Most probably topology $TOPOLOGY_NAME did not even start."
		fi
		break
	fi
	if [ $ALREADY_KILLED == "false" ]; then
		checkErrors $TOPOLOGY_NAME
		if [ "$?" == "1" ]; then
			echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			echo "ERROR: Reassigning happened in topology $TOPOLOGY_NAME. The topology will be killed..."
			echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			storm kill $TOPOLOGY_NAME
			ALREADY_KILLED=true
		fi
		if [ "$invocations" == "$FREEZED_INVOCATIONS" ]; then
			echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			echo "ERROR: Topology $TOPOLOGY_NAME executed for more than 3 hours. Considered freezed. The topology will be killed..."
			echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			storm kill $TOPOLOGY_NAME
			ALREADY_KILLED=true
		fi
	fi
	FIRST_TIME=false
	invocations+=1
	sleep $TIMEOUT_INVOKE
done
