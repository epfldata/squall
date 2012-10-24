#!/bin/bash

#The tutorial is from http://zookeeper.apache.org/doc/r3.3.3/zookeeperAdmin.html#Ongoing+Data+Directory+Cleanup.
MACHINE=squalldata@icdatasrv
MACHINE_DBGEN=${MACHINE}1

SIZE=$1

HOME=/data/squall_blade/
QUERY_PATH=data/tpchdb/
QUERY_NAME=${SIZE}G
FULL_PATH=${HOME}${QUERY_PATH}${QUERY_NAME}

ssh $MACHINE_DBGEN 'cd ' $HOME/$QUERY_PATH '; mkdir -p ' $QUERY_NAME '; cd ' $HOME '; ./dbgen -vf -s ' $SIZE ' 2>&1; mv *.tbl ' $FULL_PATH

for blade in {2..10}
do
ssh $MACHINE_DBGEN 'scp -r ' $FULL_PATH ${MACHINE}${blade}':'$HOME/$QUERY_PATH
done