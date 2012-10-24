#!/bin/bash

SYSTEM=SQUALL # possible values are SQUALL and SQUALL_PLAN_RUNNER

if [ $SYSTEM == SQUALL ] 
then
CONF_DIR=../test/squall/confs/create_confs/generated/
SQUALL_COMMAND=./squall_cluster.sh
else
CONF_DIR=../test/squall_plan_runner/confs/create_confs/generated/
SQUALL_COMMAND=./squall_plan_runner_cluster.sh
fi

RESULT_DIR=../bin/extract_results/
EXEC_LOG_DIR=$RESULT_DIR/exec_logs/
STORM_OUTPUT_PATH=$RESULT_DIR/data/

#1. deleting old data
mkdir -p $RESULT_DIR
mkdir -p $EXEC_LOG_DIR
mkdir -p $STORM_OUTPUT_PATH
rm -rf ${STORM_OUTPUT_PATH}*

# 2. for each generated file, run it and wait until it is terminated
for config in ${CONF_DIR}* ; do
        #wait until resources are freed from previous execution, the same as TOPOLOGY_MESSAGE_TIMEOUT_SECS
        #explained in Killing topology section in https://github.com/nathanmarz/storm/wiki/Running-topologies-on-a-production-cluster

	confname=${config##*/}
	$SQUALL_COMMAND $CONF_DIR/$confname > $EXEC_LOG_DIR/$confname

	#waiting for topology to finish is now in squall_cluster.sh
done

# 3. grasp output
./grasp_output.sh $STORM_OUTPUT_PATH

# 4. Extracting timing information.
CURR_DIR=`pwd`
cd $RESULT_DIR
rake -f extract_time.rb
cd $CURR_DIR