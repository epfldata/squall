#!/bin/bash

. ./storm_version.sh

CONFIG_DIR=../test/squall_plan_runner/confs/local

if [ $# -ne 1 ]
then
CONFIG_PATH=$CONFIG_DIR/0_01G_hyracks
else
CONFIG_PATH=$1
fi

cd ../deploy

# check if your configuration file exists
if ! [ -f $CONFIG_PATH ];
then
   echo "File $CONFIG_PATH does not exist! Please specify a valid configuration file!"
   exit
fi

../bin/lein run -m plan_runner.main.Main $CONFIG_PATH

#Old version implies specifying libraries explicitly
#java -Xmx128m -cp ../deploy/squall-2.0-standalone.jar:../$STORMNAME/lib/*:../contrib/*:../$STORMNAME/$STORMNAME.jar plan_runner.main.Main $CONFIG_PATH
