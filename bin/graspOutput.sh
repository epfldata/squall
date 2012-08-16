#!/bin/bash

BIN_PATH=../bin
if [ ! -d $BIN_PATH ]; then
	echo "Bin directory '$BIN_PATH' does not exit. Exiting..."
	exit
fi
. ${BIN_PATH}/storm_version.sh

MACHINE=squalldata@icdatasrv
MACHINE5=squalldata@icdatasrv5
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
for blade in {5..5}
do
  for port in {1011,1022}
  do 
    mkdir ${STORM_SUPERVISOR}${blade}-${port}
  done
done

#Grasping output from master node
scp -r $MACHINE5:$STORM_LOGS $STORM_MASTER

#Grasping output from supervisor nodes
for blade in {5..5}
do
  for port in {1011,1022}
  do
	scp -P "$port" -r $MACHINE${blade}:$STORM_LOGS ${STORM_SUPERVISOR}${blade}-${port}
  done
done