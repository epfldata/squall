#!/bin/bash

. ./storm_version.sh

MACHINE=squalldata@icdatasrv
STORM_HOME=/opt/storm
STORM_DATA=$STORM_HOME/storm_data
ZOOKEEPER_DATA=$STORM_HOME/zookeeper_data
STORM_LOGS=$STORM_HOME/$STORMNAME/logs

#Deleting all the Storm output on master node
for blade in {5..5}
do
	ssh $MACHINE$blade 'rm -r ' $STORM_DATA'/*;rm -r ' $ZOOKEEPER_DATA'/*;rm -r ' $STORM_LOGS'/*'
done

#Deleting all the Storm output on zones
for blade in {5..5}
do
  for port in {1011,1022}
  do
	ssh -p "$port" $MACHINE${blade} 'rm -r ' $STORM_DATA'/*;rm -r ' $STORM_LOGS'/*'
  done
done
