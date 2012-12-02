#!/bin/bash

if [ $# -lt 1 ]; then
MODE=SQL # possible values are SQL and PLAN_RUNNER
else
MODE=$1
fi

if [ $MODE == SQL ] 
then
CONF_DIR=../test/squall/confs/create_confs/generated/
else
CONF_DIR=../test/squall_plan_runner/confs/create_confs/generated/
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
	./squall_cluster.sh $MODE $CONF_DIR/$confname > $EXEC_LOG_DIR/$confname

	#waiting for topology to finish is now in squall_cluster.sh
done

# 3. grasp output
./grasp_output.sh $STORM_OUTPUT_PATH

# 4. Extracting timing information.
CURR_DIR=`pwd`
cd $RESULT_DIR
rake -f extract_time.rb
cd $CURR_DIR
