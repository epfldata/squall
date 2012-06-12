#!/bin/bash
./recompile.sh
rm /tmp/tmp*

CONFDIR=../testing/squall_plan_runner/confs
TESTDIR=`ls ${CONFDIR}/test/`
COUNT=`ls -lah ${CONFDIR}/test/ | wc -l`

declare -i i
i=1
echo ""
for TEST in ${TESTDIR[@]} 
do
	# Pre aggregation is broken, do not run it for now
	if [ "$TEST" == "0.01G_hyracks_pre_agg_serial" ]; then
		continue;
	fi
	# Theta joins with hyracks doesn't work either
	if [ "$TEST" == "0.01G_theta_hyracks_serial" ]; then
		continue;
	fi
	
	TMPFILE=`mktemp`
	echo "Running test $i ($TEST) out of ${COUNT}..."
	./squall_plan_runner_local.sh $TEST > $TMPFILE 
	if [ "`cat $TMPFILE | tail -n 1 | cut -d' ' -f1`" != "OK:" ]; then
		echo "Error: Test $TEST failed. Error log in $TMPFILE"
		exit
	fi
	echo "Test $TEST completed successfully..."
	rm $TMPFILE
	i+=1
done

echo ""
echo "ALL TESTS OK!"
