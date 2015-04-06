#!/bin/bash
. ./storm_env.sh

printFormat (){
	echo "Format: ./squall_local.sh CONFIG_PATH [EXTRA_JARS]"
	echo "        or"
	echo "        ./squall_local.sh MODE CONFIG_PATH [EXTRA_JARS]"
}

MODE=$1
if [[ "$MODE" != "PLAN_RUNNER" && "$MODE" != "SQL" ]]; then
	MODE=SQL
else
        shift
fi

# Set default variables according to mode
if [ "$MODE" == "PLAN_RUNNER" ]; then
	CONFIG_DIR=../test/squall/confs/local
	COMMAND=squall/runPlanner
else
	CONFIG_DIR=../test/squall_plan_runner/confs/local
	COMMAND=squall/runParser
fi

CONFIG_PATH=$1
shift
if [ "$#" -ne "0" ]; then
    EXTRA_JARS=$(realpath $@)
fi


# check if your configuration file exists
if ! [ -f $CONFIG_PATH ]; then
	echo "File $CONFIG_PATH does not exist! Please specify a valid configuration file!"
	exit 1
fi

# Now run
cd ..

sbt "$COMMAND $CONFIG_PATH $EXTRA_JARS"

#Old version implies specifying libraries explicitly
#java -Xmx128m -cp ../deploy/squall-0.2.0-standalone.jar:../$STORMNAME/lib/*:../contrib/*:../$STORMNAME/$STORMNAME.jar ch.epfl.data.sql.main.ParserMain $CONFIG_PATH
# only for 0.8
#java -Xmx128m -cp ../deploy/squall-0.2.0-standalone.jar:../$STORMNAME/lib/*:../contrib/*:../$STORMNAME/$STORMNAME.jar ch.epfl.data.plan_runner.main.Main $CONFIG_PATH
# only for 0.9
#java -Xmx128m -cp ../deploy/squall-0.2.0-standalone.jar:../$STORMNAME/lib/*:../contrib/* ch.epfl.data.plan_runner.main.Main $CONFIG_PATH
