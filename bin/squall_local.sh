#!/bin/bash
. ./storm_env.sh

printFormat (){
	echo "Format: ./squall_local.sh CONFIG_PATH"
	echo "        or"
	echo "        ./squall_local.sh MODE CONFIG_PATH"
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
	CONFIG_DIR=../test/squall/confs/local
	CONFIG_PATH=$CONFIG_DIR/0_01G_hyracks
	CLASS=ch.epfl.data.plan_runner.main.Main
else
	CONFIG_DIR=../test/squall_plan_runner/confs/local
	CONFIG_PATH=$CONFIG_DIR/0_01G_hyracks_ncl
	CLASS=ch.epfl.data.sql.main.ParserMain 
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

# Now run
cd ../deploy

../bin/lein run -m $CLASS $CONFIG_PATH

#Old version implies specifying libraries explicitly
#java -Xmx128m -cp ../deploy/squall-0.2.0-standalone.jar:../$STORMNAME/lib/*:../contrib/*:../$STORMNAME/$STORMNAME.jar ch.epfl.data.sql.main.ParserMain $CONFIG_PATH
# only for 0.8
#java -Xmx128m -cp ../deploy/squall-0.2.0-standalone.jar:../$STORMNAME/lib/*:../contrib/*:../$STORMNAME/$STORMNAME.jar ch.epfl.data.plan_runner.main.Main $CONFIG_PATH
# only for 0.9
#java -Xmx128m -cp ../deploy/squall-0.2.0-standalone.jar:../$STORMNAME/lib/*:../contrib/* ch.epfl.data.plan_runner.main.Main $CONFIG_PATH
