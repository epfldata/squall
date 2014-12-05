#!/bin/bash

#!/bin/bash

# Local settings
USERNAME=datalab
KILL_STRING=/storm/
# don't want to kill this process
NO_KILL_STRING=kill_
PS_FILE=psJava.txt

# Execution
ps -u $USERNAME -o pid,args > $PS_FILE
while read line
do
	ID=`echo $line | awk '{print $1}'`
	if [[ $line == *$KILL_STRING* && $line != *$NO_KILL_STRING* ]]; then
      # whatever is run from /storm/ path (including both Zookeeper and Storm)
  		kill -9 $ID
	fi
done < $PS_FILE
rm $PS_FILE
