#!/bin/bash
UIHOST=http://icdatasrv2.epfl.ch:8080/
WEBPAGE=index.html
SLEEP_INTERVAL=1
declare -i ACTIVE_JOB
while [ 1 ]
do
	sleep $SLEEP_INTERVAL
	wget $UIHOST 1> /dev/null 2> /dev/null
	ACTIVE_JOB=`cat $WEBPAGE | sed 's/<td>/\n<td>/g' | grep ACTIVE | wc -l`
	if [ $ACTIVE_JOB -eq 0 ]; then
		rm $WEBPAGE
		break;
	fi
	rm $WEBPAGE
done
