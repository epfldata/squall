#!/bin/bash

. ./storm_version.sh

CONFIG_DIR=../test/squall/confs/cluster

if [ $# -ne 1 ]
then
CONFIG_PATH=$CONFIG_DIR/1G_hyracks_parallel
else
CONFIG_PATH=$1
fi

# check if your configuration file exists
if ! [ -f $CONFIG_PATH ];
then
   echo "File $CONFIG_PATH does not exist! Please specify a valid configuration file!"
   exit
fi


../$STORMNAME/bin/storm jar ../deploy/squall-2.0-standalone.jar sql.main.ParserMain $CONFIG_PATH
