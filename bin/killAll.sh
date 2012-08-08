#!/bin/bash

MACHINE=squalldata@icdatasrv
PS_PATH=/export/home/squalldata/squall_scripts
PS_FILE=psJava.txt

for BLADE in {1..3}
do
	ssh $MACHINE${BLADE} 'ps -u squalldata > ' $PS_PATH/$PS_FILE
	scp $MACHINE${BLADE}:$PS_PATH/$PS_FILE $PS_FILE

	while read line
	do
		ID=`echo $line | awk '{print $1}'`
		NAME=`echo $line | awk '{print $4}'`
		if [ $NAME == "java" ]; then
			ssh $MACHINE${BLADE} 'kill -9 ' $ID
		fi
	done < $PS_FILE
	rm $PS_FILE
done

# all the local zones
for BLADE in {1..4}
do
	for ZONE in {1..22}
	do
		PORT=$((1000 + ($BLADE-1)*22+$ZONE))
		ssh -p $PORT $MACHINE${BLADE} 'ps -u squalldata > ' $PS_PATH/$PS_FILE
		scp $MACHINE${BLADE}:$PS_PATH/$PS_FILE $PS_FILE
		while read line
		do
			ID=`echo $line | awk '{print $1}'`
			NAME=`echo $line | awk '{print $4}'`
			if [ $NAME == "java" ]; then
				ssh -p $PORT $MACHINE${BLADE} 'kill -9 ' $ID
			fi
		done < $PS_FILE
		rm $PS_FILE
	done
done
