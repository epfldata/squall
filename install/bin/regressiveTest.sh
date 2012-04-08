#!/bin/bash

CONFDIR=../dip/squall/confs
TESTDIR=test/
BINDIR=../../../bin

./recompile.sh

#we have to obtain normalized file paths
cd $CONFDIR
ARRAY=(`find $TESTDIR -type f | xargs echo`) 

# go back to bin directory and run one by one config file
cd $BINDIR
for i in ${ARRAY[*]}; do
    ./squallLocalRun.sh $i
done