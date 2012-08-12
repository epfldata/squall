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

# For the storm directory
for i in {1..4}
do
	scp $SOURCE_FILE ${USERNAME}@icdatasrv${i}.epfl.ch:${CLUSTER_NODE_CONF}
	scp $SOURCE_FILE ${USERNAME}@icdatasrv${i}.epfl.ch:${INSTALL_CONF}
done

for PORT in {1001..1088}
do
	scp -P $PORT $SOURCE_FILE ${USERNAME}@icdatasrv1.epfl.ch:${CLUSTER_NODE_CONF}
done

cd $CURR_DIR
