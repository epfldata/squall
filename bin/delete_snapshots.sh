#!/bin/bash
. ./storm_version.sh

MACHINE=squalldata@icdatasrv

REMOTE_SNAP=/opt/storm/profiling/output
SNAP_LOG=/export/home/squalldata/.yjp/log

for blade in {1..10}
do
  #Deleting all the Storm output on master node
  ssh $MACHINE$blade 'rm -r ' $REMOTE_SNAP'/*'
  #Deleting log of yjp
  ssh $MACHINE$blade 'rm -r ' $SNAP_LOG'/*'
  for port in {1001..1022}
  do
    ssh -p "$port" $MACHINE$blade 'rm -r ' $REMOTE_SNAP'/*'
  done
done
