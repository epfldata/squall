#!/bin/bash

. ./storm_version.sh

if [ $# -ne 1 ]
then
CONFIG_FILE=1G_hyracks_parallel
else
CONFIG_FILE=$1
fi

CONFIG_DIR=../testing/squall/confs
CONFIG_PATH=$CONFIG_DIR/$CONFIG_FILE

# check if your configuration file exists
if ! [ -f $CONFIG_PATH ];
then
   echo "File $CONFIG_PATH does not exist! Please specify a valid configuration file!"
   exit
fi


../$STORMNAME/bin/storm jar ../deploy/squall-2.0-standalone.jar sql.main.ParserMain $CONFIG_PATH
