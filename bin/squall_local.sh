#!/bin/bash

. ./storm_version.sh

if [ $# -ne 1 ]
then
CONFIG_FILE=0.01G_hyracks_ncl_serial
else
CONFIG_FILE=$1
fi

cd ../deploy

CONFIG_DIR=../testing/squall/confs
CONFIG_PATH=$CONFIG_DIR/$CONFIG_FILE

# check if your configuration file exists
if ! [ -f $CONFIG_PATH ];
then
   echo "File $CONFIG_PATH does not exist! Please specify a valid configuration file!"
   exit
fi

../bin/lein run -m sql.main.ParserMain $CONFIG_PATH

#Old version implies specifying libraries explicitly
#java -cp ../deploy/squall-2.0-standalone.jar:../$STORMNAME/lib/*:../contrib/*:../$STORMNAME/$STORMNAME.jar main.ParserMain $CONFIG_PATH