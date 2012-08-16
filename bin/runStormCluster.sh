#!/bin/bash
. ./storm_version.sh

MACHINE5=squalldata@icdatasrv5
STORMPATH=/opt/storm/$STORMNAME
LOGPATH=$STORMPATH/logs

# Multi-server zookeeper
#echo "1" | ssh $MACHINE1 'cat > /opt/storm/zookeeper_data/myid'
#echo "2" | ssh $MACHINE3 'cat > /opt/storm/zookeeper_data/myid'
#ssh $MACHINE1 '/opt/storm/bin/zookeeperd > /opt/storm/zookeeper_data/consoleZoo.txt 2>&1 &'
#ssh $MACHINE3 '/opt/storm/bin/zookeeperd > /opt/storm/zookeeper_data/consoleZoo.txt 2>&1 &'

#All of the processes run in background mode (& at the end of each command)
#By default, all the processes on Solaris are run under supervision, using SMF tool
#Running zookeeper, nimbus and ui on a single node
#Running zookeeper
ssh $MACHINE5 '/opt/storm/bin/zookeeperd > /opt/storm/zookeeper_data/consoleZoo.txt 2>&1 &'

#Running nimbus and ui
#Console output goes to the current directory (pwd).
#logs directory goes to pwd/logs.
ssh $MACHINE5 $STORMPATH'/bin/storm nimbus > ' $LOGPATH'/consoleNimbus.txt 2>&1 &'
ssh $MACHINE5 $STORMPATH'/bin/storm ui > ' $LOGPATH'/consoleUI.txt 2>&1 &'

#Running supervisors on all the nodes.
ssh -p 1011 $MACHINE5 $STORMPATH'/bin/storm supervisor > ' $LOGPATH'/consoleSupervisor.txt 2>&1 &'
ssh -p 1022 $MACHINE5 $STORMPATH'/bin/storm supervisor > ' $LOGPATH'/consoleSupervisor.txt 2>&1 &'