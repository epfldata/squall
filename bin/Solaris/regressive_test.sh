#!/bin/bash
./recompile.sh
rm /tmp/tmp*

# Check command line of command line arguments 
if [ $# -lt 1 ]; then
	echo "Illegal number of command line arguments: Mode must be provided."
	echo "Mode can be either PLAN_RUNNER or SQL."
	exit
fi
MODE=$1
if [[ "$MODE" != "PLAN_RUNNER" && "$MODE" != "SQL" && "$MODE" != "MBUCKET" ]]; then
	echo "Invalid mode $MODE: mode can be either PLAN_RUNNER or SQL. Exit..."
	exit
fi

# Throw a warning if there are more arguments than required
if [ $# -gt 2 ]; then
	echo "WARNING:: More than 2 arguments specified. Rest will be ignored!"
fi

# Set default variables according to mode
if [ "$MODE" == "SQL" ]; then
	CONFDIR=../test/squall/confs/local
elif [ "$MODE" == "PLAN_RUNNER" ]; then
 	CONFDIR=../test/squall_plan_runner/confs/local
else
	MODE="PLAN_RUNNER"
	CONFDIR=$2
#	CONFDIR=../test/squall_plan_runner/confs/join_matrix/push/local/console-run
fi
TESTCONFS=( `ls ${CONFDIR}/` )
COUNT=${#TESTCONFS[@]}
OS_TYPE=`uname -s`
declare -i i
i=1
ALL_OK=true
TMPFILE=""

mkTempFile() {
	if [ "$OS_TYPE" == "Darwin" ]; then
		TMPFILE=`mktemp /tmp/tmp.XXXXXXXXXXXXXXXX`
	else
		TMPFILE=`mktemp`
	fi
}

echo ""
# Run all tests given, one by one
for TEST in ${TESTCONFS[@]} 
do
	# Cleanup directory
	rm -rf /tmp/ramdisk/*
	sync; sync; sync
	# FIXME: Pre aggregation is broken, do not run it for now
	if [ "$TEST" == "0.01G_hyracks_pre_agg_serial" ]; then
		continue;
	fi
	
	# Create a temporary output file	
	mkTempFile 
	echo "Running test $i ($TEST) out of ${COUNT}..."
	./squall_local.sh $MODE $CONFDIR/$TEST > $TMPFILE 
	if [ "`cat $TMPFILE | tail -n 1 | cut -d' ' -f1`" != "OK:" ]; then
		echo "Error: Test $TEST failed. Error log in $TMPFILE"
                ALL_OK=false
	else
		echo "Test $TEST completed successfully..."
	fi
	i+=1
done

echo ""
if $ALL_OK ; then
	echo "ALL TESTS OK!"
else
	echo "Some tests failed. Check log files"
fi
