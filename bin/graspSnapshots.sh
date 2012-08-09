#!/bin/bash
MACHINE1=squalldata@icdatasrv1
MACHINE2=squalldata@icdatasrv2
MASTER=master
SUPERVISOR=icdatasrv

REMOTE_SNAP=/opt/storm/profiling/output/*
LOCAL_SNAP=snapshots

removeIfEmpty(){
	DIR=$1
	if [ `ls -A $DIR | wc -l` == 0 ]
	then 
		rm -r $DIR
	fi
}

mkdir $LOCAL_SNAP
cd $LOCAL_SNAP

#Grasping from master node
mkdir $MASTER
scp -r $MACHINE2:$REMOTE_SNAP ${LOCAL_SNAP}/$MASTER
removeIfEmpty "$MASTER"

#Grasping output from supervisor nodes
for BLADE in {1..4}
do
	for ZONE in {1..22}
	do
		mkdir ${SUPERVISOR}${BLADE}-${ZONE}
		PORT=$((1000 + ($BLADE-1)*22+$ZONE))
		scp -P $PORT -r $MACHINE1:$REMOTE_SNAP ${SUPERVISOR}${BLADE}-${ZONE}/
		removeIfEmpty "${SUPERVISOR}${BLADE}-${ZONE}"
	done
done
