#!/bin/bash

if [ $# -ne 1 ]
then
CONFIG_FILE=0.1G_hyracks_serial
else
CONFIG_FILE=$1
fi

CONFIG_DIR=../dip/Squall/confs
CONFIG_PATH=$CONFIG_DIR/$CONFIG_FILE

# check if your configuration file exists
if ! [ -f $CONFIG_PATH ];
then
   echo "File $CONFIG_PATH does not exist! Please specify a valid configuration file!"
   exit
fi

java -cp ../compilation/squall-2.0-standalone.jar:../storm-0.7.0/lib/*:../storm-0.7.0/storm-0.7.0.jar main.Main $CONFIG_PATH