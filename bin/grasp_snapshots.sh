#!/bin/bash

MACHINE=squalldata@icdatasrv
MACHINE5=${MACHINE}5
REMOTE_SNAP=/opt/storm/profiling/output/*

if [ $# -ne 1 ]
then
  LOCAL_SNAP=snapshots
  mkdir -p $LOCAL_SNAP
else
  LOCAL_SNAP=$1
fi

if [ ! -d $STORM_LOCAL ]; then
  echo "Directory '$STORM_LOCAL' does not exist. Exiting..."
  exit
fi

MASTER=$LOCAL_SNAP/master
SUPERVISOR=$LOCAL_SNAP/icdatasrv

removeIfEmpty(){
	DIR=$1
	if [ `ls -A $DIR | wc -l` == 0 ]
	then 
		rm -r $DIR
	fi
}

#Grasping from master node
mkdir -p $MASTER
scp -r $MACHINE5:$REMOTE_SNAP $MASTER
removeIfEmpty "$MASTER"

#Grasping output from supervisor nodes
for blade in {1..10}
do
	for port in {1001..1022}
	do
		supervisor=${SUPERVISOR}${blade}-${port}
		mkdir -p $supervisor
		scp -P $port -r $MACHINE${blade}:$REMOTE_SNAP $supervisor
		removeIfEmpty "$supervisor"
	done
done
