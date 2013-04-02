#!/bin/bash

. ./storm_version.sh

USERNAME=squalldata
MACHINE=$USERNAME@icdatasrv
LOCAL_ZOO_CFG_PATH=$1
CLUSTER_ZOO_CFG_PATH=/opt/storm/conf/

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
  scp $LOCAL_ZOO_CFG_PATH ${MACHINE}${blade}:${CLUSTER_ZOO_CFG_PATH}
  for port in {1001..1022}
  do
	scp -P $port $LOCAL_ZOO_CFG_PATH ${MACHINE}${blade}:${CLUSTER_ZOO_CFG_PATH}
  done
done
