#!/bin/bash
./recompile.sh

# Check command line of command line arguments 
if [ $# -lt 2 ]; then
	echo "Illegal number of command line arguments"
	echo "Usage: MODE PATH"
	exit
fi
MODE=$1
CONFDIR=$2
OUTPUTDIR=../test/m_bucket/optimality_logs/

rm $OUTPUTDIR/log*

TESTCONFS=( `ls ${CONFDIR}/` )
COUNT=${#TESTCONFS[@]}
OS_TYPE=`uname -s`
declare -i i
i=1
ALL_OK=true
TMPFILE=""

echo ""
# Run all tests given, one by one
for TEST in ${TESTCONFS[@]} 
do
	echo "Running test $i ($TEST) out of ${COUNT}..."
	./squall_local.sh $MODE $CONFDIR/$TEST > $OUTPUTDIR/log_$TEST
	if [ "`cat $OUTPUTDIR/log_$TEST | tail -n 1 | cut -d' ' -f1`" != "OK:" ]; then
		echo "Error: Test $TEST failed."
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
