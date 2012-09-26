#!/bin/bash
# An invoker is responsible for cleaning up STORM_LOCAL directory.

. ./storm_version.sh

MACHINE=squalldata@icdatasrv
MACHINE5=${MACHINE}5
STORM_HOME=/opt/storm
STORM_DATA=$STORM_HOME/storm_data
ZOOKEEPER_DATA=$STORM_HOME/zookeeper_data
STORM_LOGS=$STORM_HOME/$STORMNAME/logs

if [ $# -ne 1 ]
then
  STORM_LOCAL=storm_output
  mkdir -p $STORM_LOCAL
else
  STORM_LOCAL=$1
fi

if [ ! -d $STORM_LOCAL ]; then
  echo "Directory '$STORM_LOCAL' does not exist. Exiting..."
  exit
fi

STORM_MASTER=$STORM_LOCAL/master
STORM_SUPERVISOR=$STORM_LOCAL/supervisor

#Grasping output from master node
mkdir -p $STORM_MASTER
scp -r $MACHINE5:$STORM_LOGS $STORM_MASTER

#Grasping output from supervisor nodes
for blade in {1..10}
do
  for port in {1001..1022}
  do
	supervisor=${STORM_SUPERVISOR}${blade}-${port}
	mkdir -p $supervisor
	scp -P "$port" -r $MACHINE${blade}:$STORM_LOGS $supervisor
  done
done
