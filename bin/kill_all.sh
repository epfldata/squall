#!/bin/bash

MACHINE=squalldata@icdatasrv
PS_PATH=/data/squall_zone/temp
PS_FILE=psJava.txt

echo "Killing global zones..."
for blade in {1..10}
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
	ssh $MACHINE${blade} 'rm -rf ' $PS_PATH/$PS_FILE
done

# all the local zones
echo "Killing local zones..."
for blade in {1..10}
do
	for port in {1001..1022}
	do
		ssh -p $port $MACHINE${blade} 'ps -u squalldata > ' $PS_PATH/$PS_FILE
		scp -P $port $MACHINE${blade}:$PS_PATH/$PS_FILE $PS_FILE

		while read line
		do
			ID=`echo $line | awk '{print $1}'`
			NAME=`echo $line | awk '{print $4}'`
			if [ $NAME == "java" ]; then
				ssh -p $port $MACHINE${blade} 'kill -9 ' $ID
			fi
		done < $PS_FILE
		rm $PS_FILE
		ssh -p $port $MACHINE${blade} 'rm -rf ' $PS_PATH/$PS_FILE
	done
done