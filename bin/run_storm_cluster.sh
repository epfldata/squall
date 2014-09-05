#!/bin/bash
. ./storm_env.sh

# ********** LOCAL PARAMS ***********
#If changed zookeeper locations/number, do a change here and on one more place
ZOO1=3
ZOO2=7
ZOOBLADE1=blade03
ZOOBLADE2=blade07
# ********** END OF LOCAL PARAMS ***********

# ********** RUNNING STUFF **********
#All of the processes run in background mode (& at the end of each command)

#Running Zookeeper
#If changed zookeeper locations/number, do a change here and on one more place
ssh $MASTER cexec :$ZOO1  echo 1 '">"' $ZOOKEEPERPATH/myid
ssh $MASTER cexec :$ZOO2  echo 2 '">"' $ZOOKEEPERPATH/myid
ssh $MASTER ssh $ZOOBLADE1 $STORM_INSTALL_DIR/bin/zookeeperd '">"' $ZOOKEEPERPATH/consoleZoo.txt '"2>&1 &"'
ssh $MASTER ssh $ZOOBLADE2 $STORM_INSTALL_DIR/bin/zookeeperd '">"' $ZOOKEEPERPATH/consoleZoo.txt '"2>&1 &"'

#Running nimbus and ui
ssh $MASTER $STORMPATH/bin/storm nimbus '>' $STORM_LOGPATH/consoleNimbus.txt '2>&1 &'
ssh $MASTER $STORMPATH/bin/storm ui '>' $STORM_LOGPATH/consoleUI.txt '2>&1 &'

#Running supervisors on all the nodes.
ssh $MASTER cexec :$BLADES $STORMPATH/bin/storm supervisor '">"' $STORM_LOGPATH/consoleSupervisor.txt '"2>&1 &"'

#Running logviewers on all the nodes.
ssh $MASTER cexec :$BLADES $STORMPATH/bin/storm logviewer '">"' $STORM_LOGPATH/consoleLogViewer.txt '"2>&1 &"'
# ********** END OF RUNNING STUFF **********




# ********** END OF FILE: COMMENTS **********
# These all worked on the cluster
#### cexec :3 echo 1 '>' $ZOOKEEPERPATH/myid
#### echo "1" | cexec :3 'cat > '$ZOOKEEPERPATH'/myid'
#### cexec :3 'echo "1" > '$ZOOKEEPERPATH'/myid'
#### cexec :3 'echo 1 > '$ZOOKEEPERPATH'/myid'
#### cexec :3 'echo '1' > '$ZOOKEEPERPATH'/myid'
#### cexec :3 "echo 1 > /data/lab/storm_tmp/zookeeper_data/myid"
# These all worked from office machine (">" because we don't want to redirect on $MASTER but on blades)
#### echo 1 | ssh $MASTER cexec :$ZOO1  cat '">"' $ZOOKEEPERPATH/myid
#### ssh $MASTER 'cexec :'$ZOO1 ' echo 1 ">"' $ZOOKEEPERPATH'/myid'
#### echo 1 | ssh $MASTER 'cexec :'$ZOO1 ' cat ">"' $ZOOKEEPERPATH'/myid'
#### ssh $MASTER 'cexec :3 echo 1 ">"' $ZOOKEEPERPATH'/myid'
#### ssh $MASTER 'cexec :3 echo 1 ">"'$ZOOKEEPERPATH'/myid'
#### ssh $MASTER 'cexec :3 "echo 1 > "'$ZOOKEEPERPATH'"/myid"'
