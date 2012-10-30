#!/bin/bash
. ./storm_version.sh

# Check command line of command line arguments 
if [ $# -lt 1 ]; then
	echo "Illegal number of command line arguments: Mode must be provided."
	echo "Mode can be either PLAN_RUNNER or SQL."
	exit
fi
MODE=$1
if [[ "$MODE" != "PLAN_RUNNER" && "$MODE" != "SQL" ]]; then
	echo "Invalid mode $MODE: mode can be either PLAN_RUNNER or SQL. Exit..."
	exit
fi

# Set default variables according to mode
if [ "$MODE" == "PLAN_RUNNER" ]; then
	CONFIG_DIR=../test/squall/confs/local
	CONFIG_PATH=$CONFIG_DIR/0_01G_hyracks
	CLASS=plan_runner.main.Main
else
	CONFIG_DIR=../test/squall_plan_runner/confs/local
	CONFIG_PATH=$CONFIG_DIR/0_01G_hyracks_ncl
	CLASS=sql.main.ParserMain 
fi

# But if user has specified a specific configuration file, run this
if [ $# -lt 3 ]; then
	CONFIG_PATH=$2
	# check if your configuration file exists
	if ! [ -f $CONFIG_PATH ]; then
		echo "File $CONFIG_PATH does not exist! Please specify a valid configuration file!"
		exit
	fi
fi

# Throw a warning if there are more arguments than required
if [ $# -gt 2 ]; then
	echo "WARNING:: More than 2 arguments specified. Rest will be ignored!"
fi

# Now run
cd ../deploy

../bin/lein run -m $CLASS $CONFIG_PATH

#Old version implies specifying libraries explicitly
#java -Xmx128m -cp ../deploy/squall-0.2.0-standalone.jar:../$STORMNAME/lib/*:../contrib/*:../$STORMNAME/$STORMNAME.jar sql.main.ParserMain $CONFIG_PATH
#java -Xmx128m -cp ../deploy/squall-0.2.0-standalone.jar:../$STORMNAME/lib/*:../contrib/*:../$STORMNAME/$STORMNAME.jar plan_runner.main.Main $CONFIG_PATH
