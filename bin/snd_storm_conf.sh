#!/bin/bash
. ./storm_env.sh

CLUSTER_HOME=/localhome/datalab/

STORM_CONF_NAME=storm.yaml
CLUSTER_INSTALL_STORM_CONF=$STORMPATH/conf/$STORM_CONF_NAME
LOCAL_HOME_STORM_CONF=~/.storm/$STORM_CONF_NAME
CLUSTER_HOME_STORM_CONF=$CLUSTER_HOME/.storm/$STORM_CONF_NAME

SOURCE_FILE=$1
if [ ! -f $SOURCE_FILE ]; then
	echo $SOURCE_FILE "is not a file. Exiting..."
	exit
fi

# We have to change our local ~/.storm directory
cp $SOURCE_FILE $LOCAL_HOME_STORM_CONF
# we have to replace master3 to icdataportal3 on local machine
../bin/adjust_storm_yaml_locally.sh

# Copy it to Master
scp $SOURCE_FILE $MASTER:$CLUSTER_INSTALL_STORM_CONF
scp $SOURCE_FILE $MASTER:$CLUSTER_HOME_STORM_CONF
# Copy it on Blades
ssh $MASTER cpush $CLUSTER_HOME_STORM_CONF $CLUSTER_HOME_STORM_CONF
ssh $MASTER cpush $CLUSTER_INSTALL_STORM_CONF $CLUSTER_INSTALL_STORM_CONF
