#!/bin/bash

. ./storm_version.sh

USERNAME=squalldata
MACHINE=$USERNAME@icdatasrv

CLUSTER_NODE_CONF=/opt/storm/$STORMNAME/conf/storm.yaml
CLUSTER_HOME_CONF=/export/home/$USERNAME/.storm/storm.yaml

RESOURCES_DIR=../resources
if [ ! -d $RESOURCES_DIR ]; then
	echo "Resources directory (containing storm.yaml and storm.yaml.profiling) does not exist. Exiting..."
	exit
fi

SOURCE_FILE=$RESOURCES_DIR/$1

# You have as well to change in your local ~/.storm directory
cp $SOURCE_FILE ~/.storm/storm.yaml
# we have to replace icdatasrv5-priv to icdatarv5 on local machine
../bin/adjust_storm_yaml_locally.sh

# For the storm directory
for blade in {1..10}
do
  scp $SOURCE_FILE ${MACHINE}${blade}:${CLUSTER_NODE_CONF}
  scp $SOURCE_FILE ${MACHINE}${blade}:${CLUSTER_HOME_CONF}
  for port in {1001..1022}
  do
	scp -P $port $SOURCE_FILE ${MACHINE}${blade}:${CLUSTER_NODE_CONF}
  done
done
