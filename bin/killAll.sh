#!/bin/bash

MACHINE=squalldata@icdatasrv
PS_PATH=/export/home/squalldata/squall_scripts
PS_FILE=psJava.txt

for blade in {5..5}
do
	ssh $MACHINE${blade} 'ps -u squalldata > ' $PS_PATH/$PS_FILE
	scp $MACHINE${blade}:$PS_PATH/$PS_FILE $PS_FILE

	while read line
	do
		ID=`echo $line | awk '{print $1}'`
		NAME=`echo $line | awk '{print $4}'`
		if [ $NAME == "java" ]; then
			ssh $MACHINE${blade} 'kill -9 ' $ID
		fi
	done < $PS_FILE
	rm $PS_FILE
done

# all the local zones
for blade in {5..5}
do
	for port in {1011,1022}
	do
		ssh -p $port $MACHINE${blade} 'ps -u squalldata > ' $PS_PATH/$PS_FILE
		scp $MACHINE${blade}:$PS_PATH/$PS_FILE $PS_FILE
		while read line
		do
			ID=`echo $line | awk '{print $1}'`
			NAME=`echo $line | awk '{print $4}'`
			if [ $NAME == "java" ]; then
				ssh -p $port $MACHINE${blade} 'kill -9 ' $ID
			fi
		done < $PS_FILE
		rm $PS_FILE
	done
done