#!/bin/bash

. ./storm_version.sh

MACHINE=team11@icdatasrv2
STORM_HOME=/opt/storm
STORM_DATA=$STORM_HOME/storm_data
ZOOKEEPER_DATA=$STORM_HOME/zookeeper_data
STORM_LOGS=$STORM_HOME/$STORMNAME/logs

STORM_LOCAL=stormOutput
STORM_MASTER=master
STORM_SUPERVISOR=supervisor

mkdir $STORM_LOCAL
cd $STORM_LOCAL
mkdir $STORM_MASTER
for i in {1001..1088}
do 
mkdir ${STORM_SUPERVISOR}${i}
done

#Grasping output from master node
scp -r $MACHINE:$STORM_LOGS $STORM_MASTER

#Grasping output from supervisor nodes
for PORT in {1001..1088}
do
	scp -P "$PORT" -r $MACHINE:$STORM_LOGS ${STORM_SUPERVISOR}${PORT}
done