#!/bin/bash

. ./storm_version.sh

USERNAME=squalldata
MACHINE=$USERNAME@icdatasrv

LOCAL_BASE_PATH=$1
LOCAL_STORM_PATH=$LOCAL_BASE_PATH/${STORMNAME}.jar
LOCAL_LOGGER_PATH=$LOCAL_BASE_PATH/storm.log.properties
CLUSTER_STORM_PATH=/opt/storm/$STORMNAME/
CLUSTER_LOGGER_PATH=$CLUSTER_STORM_PATH/log4j/

function usage() {
	echo "Usage:      ./snd_zoo.sh LOCAL_ZOO_CFG_PATH"
	exit
}


# Check correct number of command line arguments
if [ $# -ne 1 ]; then
	echo "Error: Illegal number of command line arguments. Required 1 argument and got $#. Exiting..."
	usage
fi

# For the storm directory
for blade in {1..10}
do
  scp $LOCAL_LOGGER_PATH ${MACHINE}${blade}:${CLUSTER_LOGGER_PATH}
  scp $LOCAL_STORM_PATH ${MACHINE}${blade}:${CLUSTER_STORM_PATH}
  for port in {1001..1022}
  do
    scp -P $port $LOCAL_LOGGER_PATH ${MACHINE}${blade}:${CLUSTER_LOGGER_PATH}
    scp -P $port $LOCAL_STORM_PATH ${MACHINE}${blade}:${CLUSTER_STORM_PATH}
  done
done
