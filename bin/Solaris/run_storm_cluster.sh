#!/bin/bash
. ./storm_version.sh

MACHINE=squalldata@icdatasrv
MACHINE5=squalldata@icdatasrv5
ZOO1=squalldata@icdatasrv9
ZOO2=squalldata@icdatasrv7
STORMPATH=/opt/storm/$STORMNAME
LOGPATH=/data/squall_zone/logs

#All of the processes run in background mode (& at the end of each command)
#By default, all the processes on Solaris are run under supervision, using SMF tool

# Multi-server zookeeper
echo "1" | ssh $ZOO1 'cat > /data/squall_zone/zookeeper_data/myid'
echo "2" | ssh $ZOO2 'cat > /data/squall_zone/zookeeper_data/myid'
ssh $ZOO1 '/opt/storm/bin/zookeeperd > /data/squall_zone/zookeeper_data/consoleZoo.txt 2>&1 &'
ssh $ZOO2 '/opt/storm/bin/zookeeperd > /data/squall_zone/zookeeper_data/consoleZoo.txt 2>&1 &'


#Running nimbus and ui
#Console output goes to the current directory (pwd).
#logs directory goes to pwd/logs.
ssh $MACHINE5 $STORMPATH'/bin/storm nimbus > ' $LOGPATH'/consoleNimbus.txt 2>&1 &'
ssh $MACHINE5 $STORMPATH'/bin/storm ui > ' $LOGPATH'/consoleUI.txt 2>&1 &'

#Running supervisors on all the nodes.
for blade in {1..10}
do
  for port in {1001..1022}
  do
    ssh -p $port $MACHINE$blade $STORMPATH'/bin/storm supervisor > ' $LOGPATH'/consoleSupervisor.txt 2>&1 &'
  done 
done
