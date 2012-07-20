#!/bin/bash
. ./storm_version.sh

MACHINE=squalldata@icdatasrv2
STORMPATH=/opt/storm/$STORMNAME
LOGPATH=$STORMPATH/logs

MACHINE1=squalldata@icdatasrv1
MACHINE3=squalldata@icdatasrv3

#All of the processes run in background mode (& at the end of each command)
#By default, all the processes on Solaris are run under supervision, using SMF tool
#Running zookeeper, nimbus and ui on a single node
#Running zookeeper
echo "1" | ssh $MACHINE1 'cat > /opt/storm/zookeeper_data/myid'
echo "2" | ssh $MACHINE3 'cat > /opt/storm/zookeeper_data/myid'
ssh $MACHINE1 '/opt/storm/bin/zookeeperd > /opt/storm/zookeeper_data/consoleZoo.txt 2>&1 &'
ssh $MACHINE3 '/opt/storm/bin/zookeeperd > /opt/storm/zookeeper_data/consoleZoo.txt 2>&1 &'

#Running nimbus and ui
#Console output goes to the current directory (pwd).
#logs directory goes to pwd/logs.
ssh $MACHINE $STORMPATH'/bin/storm nimbus > ' $LOGPATH'/consoleNimbus.txt 2>&1 &'
ssh $MACHINE $STORMPATH'/bin/storm ui > ' $LOGPATH'/consoleUI.txt 2>&1 &'

#Running supervisors on all the 88 nodes.
for PORT in {1001..1088}
do
	ssh -p "$PORT" $MACHINE $STORMPATH'/bin/storm supervisor > ' $LOGPATH'/consoleSupervisor.txt 2>&1 &'
done
