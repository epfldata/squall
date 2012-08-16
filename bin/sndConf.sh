#!/bin/bash

. ./storm_version.sh

USERNAME=squalldata
SOURCE_FILE=$1
CLUSTER_NODE_CONF=/opt/storm/$STORMNAME/conf/storm.yaml
CLUSTER_HOME_CONF=/export/home/$USERNAME/.storm/storm.yaml

RESOURCES_DIR=../resources
if [ ! -d $RESOURCES_DIR ]; then
	echo "Resources directory (containing storm.yaml and storm.yaml.profiling) does not exist. Exiting..."
	exit
fi
CURR_DIR=`pwd`

cd $RESOURCES_DIR


# You have as well to change in your local ~/.storm directory
cp $SOURCE_FILE ~/.storm
# we have to replace icdatasrv5-priv to icdatarv5 on local machine
../bin/adjust_storm_yaml_locally.sh $1

# For the storm directory
for blade in {5..5}
do
	scp $SOURCE_FILE ${USERNAME}@icdatasrv${blade}:${CLUSTER_NODE_CONF}
	scp $SOURCE_FILE ${USERNAME}@icdatasrv${blade}:${CLUSTER_HOME_CONF}
done

for blade in {5..5}
do
  for port in {1011,1022}
  do
	scp -P $port $SOURCE_FILE ${USERNAME}@icdatasrv${blade}:${CLUSTER_NODE_CONF}
  done
done

cd $CURR_DIR