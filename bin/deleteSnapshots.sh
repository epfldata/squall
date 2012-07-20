#!/bin/bash
BIN_PATH=/home/klonatos/Desktop/squall/squall/bin
if [ ! -d $BIN_PATH ]; then
	echo "Bin directory '$BIN_PATH' does not exit. Exiting..."
	exit
fi
. ${BIN_PATH}/storm_version.sh

MACHINE=squalldata@icdatasrv
MACHINE2=squalldata@icdatasrv2
REMOTE_SNAP=/opt/storm/profiling/output
SNAP_LOG=/export/home/squalldata/.yjp/log

for BLADE in {1..4}
do
#Deleting all the Storm output on master node
ssh $MACHINE$BLADE 'rm -r ' $REMOTE_SNAP'/*'
#Deleting log of yjp
ssh $MACHINE$BLADE 'rm -r ' $SNAP_LOG'/*'
done

#Deleting all the Storm output on zones
for PORT in {1001..1088}
do
	ssh -p "$PORT" $MACHINE2 'rm -r ' $REMOTE_SNAP'/*'
done
