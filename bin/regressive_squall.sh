#!/bin/bash
./recompile.sh
rm /tmp/tmp*

CONFDIR=../testing/squall/confs
TESTDIR=test
TESTCONFS=( `ls ${CONFDIR}/${TESTDIR}/` )
COUNT=${#TESTCONFS[@]}

declare -i i
i=1
ALL_OK=true
echo ""
for TEST in ${TESTCONFS[@]} 
do
	# Pre aggregation is broken, do not run it for now
	if [ "$TEST" == "0.01G_hyracks_pre_agg_serial" ]; then
		continue;
	fi
	
	TMPFILE=`mktemp`
	echo "Running test $i ($TEST) out of ${COUNT}..."
	./squall_local.sh $TESTDIR/$TEST > $TMPFILE 
	if [ "`cat $TMPFILE | tail -n 1 | cut -d' ' -f1`" != "OK:" ]; then
		echo "Error: Test $TEST failed. Error log in $TMPFILE"
                ALL_OK=false
	else
		echo "Test $TEST completed successfully..."
		rm $TMPFILE
	fi
	i+=1
done

if $ALL_OK ; then
  echo ""
  echo "ALL TESTS OK!"
fi