#!/bin/bash

./recompile.sh
. ./storm_version.sh

CONFIG_DIR=../test/squall/confs/cluster

# If no configuration file is given, use default
if [ $# -ne 1 ]
then
  CONFIG_PATH=$CONFIG_DIR/1G_hyracks
else
  CONFIG_PATH=$1
fi

# check if your configuration file exists
if ! [ -f $CONFIG_PATH ];
then
   echo "File $CONFIG_PATH does not exist! Please specify a valid configuration file!"
   exit
fi

confname=${CONFIG_PATH##*/}

../$STORMNAME/bin/storm jar ../deploy/squall-0.2.0-standalone.jar sql.main.ParserMain $CONFIG_PATH
TIME_BEFORE="$(date +%s)"
./wait_topology.sh $confname
TIME_AFTER="$(date +%s)"
ELAPSED_TIME="$(expr $TIME_AFTER - $TIME_BEFORE)"
echo | awk -v D=$ELAPSED_TIME '{printf "Job Elapsed Time (this includes time after receiving kill signal, but not disappearing from Web UI): %02d:%02d:%02d\n",D/(60*60),D%(60*60)/60,D%60}'