#!/bin/bash

. ./storm_version.sh

printFormat (){
	echo "Format: ./squall_cluster.sh CONFIG_PATH"
	echo "        or"
	echo "        ./squall_cluster.sh MODE CONFIG_PATH"
}

# Throw an error if there are more arguments than required
if [[ $# -gt 2 || $# -lt 1 ]]; then
	echo "ERROR:: Inproper number of arguments!"
	printFormat
	exit
fi

MODE=$1
AUTO_MODE=false
if [[ "$MODE" != "PLAN_RUNNER" && "$MODE" != "SQL" ]]; then
	MODE=SQL
	AUTO_MODE=true
fi

# Set default variables according to mode
if [ "$MODE" == "PLAN_RUNNER" ]; then
	CONFIG_DIR=../test/squall/confs/cluster
	CONFIG_PATH=$CONFIG_DIR/1G_hyracks
	CLASS=plan_runner.main.Main
else
	CONFIG_DIR=../test/squall_plan_runner/confs/cluster
	CONFIG_PATH=$CONFIG_DIR/1G_hyracks
	CLASS=sql.main.ParserMain 
fi

# But if user has specified a specific configuration file, run this
if [ $# -eq 2 ]; then
	CONFIG_PATH=$2
else if [[ $# -eq 1 && "$AUTO_MODE" == "true" ]]; then
	CONFIG_PATH=$1
     else
	echo "ERROR:: Format not followed:"
	printFormat
	exit
     fi
fi
# check if your configuration file exists
if ! [ -f $CONFIG_PATH ]; then
	echo "File $CONFIG_PATH does not exist! Please specify a valid configuration file!"
	exit
fi

confname=${CONFIG_PATH##*/}

../$STORMNAME/bin/storm jar ../deploy/squall-0.2.0-standalone.jar $CLASS $CONFIG_PATH
TIME_BEFORE="$(date +%s)"
./wait_topology.sh $confname
TIME_AFTER="$(date +%s)"
ELAPSED_TIME="$(expr $TIME_AFTER - $TIME_BEFORE)"
echo | awk -v D=$ELAPSED_TIME '{printf "Job Elapsed Time (this includes time after receiving kill signal, but not disappearing from Web UI): %02d:%02d:%02d\n",D/(60*60),D%(60*60)/60,D%60}'
