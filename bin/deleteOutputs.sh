#!/bin/bash

. ./storm_version.sh

MACHINE=squalldata@icdatasrv
MACHINE2=squalldata@icdatasrv2
STORM_HOME=/opt/storm
STORM_DATA=$STORM_HOME/storm_data
ZOOKEEPER_DATA=$STORM_HOME/zookeeper_data
STORM_LOGS=$STORM_HOME/$STORMNAME/logs

#Deleting all the Storm output on master node
for BLADE in {1..3}
do
	ssh $MACHINE$BLADE 'rm -r ' $STORM_DATA'/*;rm -r ' $ZOOKEEPER_DATA'/*;rm -r ' $STORM_LOGS'/*'
done

#Deleting all the Storm output on zones
for PORT in {1001..1088}
do
	ssh -p "$PORT" $MACHINE2 'rm -r ' $STORM_DATA'/*;rm -r ' $STORM_LOGS'/*'
done
