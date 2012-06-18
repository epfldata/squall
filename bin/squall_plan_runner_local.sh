#!/bin/bash

if [ $# -ne 1 ]
then
CONFIG_FILE=0.01G_hyracks_serial
else
CONFIG_FILE=$1
fi

CONFIG_DIR=../testing/squall_plan_runner/confs
CONFIG_PATH=$CONFIG_DIR/$CONFIG_FILE

# check if your configuration file exists
if ! [ -f $CONFIG_PATH ];
then
   echo "File $CONFIG_PATH does not exist! Please specify a valid configuration file!"
   exit
fi

java -cp ../squall-2.0-standalone.jar:../storm-0.7.0/lib/*:../contrib/*:../storm-0.7.0/storm-0.7.0.jar -Xmx1024m main.Main $CONFIG_PATH