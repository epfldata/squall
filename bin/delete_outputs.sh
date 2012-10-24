#!/bin/bash

. ./storm_version.sh

MACHINE=squalldata@icdatasrv
STORM_HOME=/data/squall_zone

STORM_DATA=$STORM_HOME/storm_data
ZOOKEEPER_DATA=$STORM_HOME/zookeeper_data
STORM_LOGS=$STORM_HOME/logs

#Deleting all the Storm output on master + zones
for blade in {1..10}
do
  ssh $MACHINE$blade 'rm -r ' $STORM_DATA'/*;rm -r ' $ZOOKEEPER_DATA'/*;rm -r ' $STORM_LOGS'/*'
  for port in {1001..1022}
  do
	ssh -p "$port" $MACHINE${blade} 'rm -r ' $STORM_DATA'/*;rm -r ' $STORM_LOGS'/*'
  done
done
