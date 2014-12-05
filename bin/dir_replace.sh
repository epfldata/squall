#!/bin/bash

# configs
BASELINE_DIR=$1
FIND=$2
REPLACE=$3
# end of configs

printFormat (){
	echo "Format: ./dir_replace.sh BASELINE_DIR FIND REPLACE"
}

# Throw an error in case of improper number of arguments
if [[ $# -gt 3 || $# -lt 3 ]]; then
	echo "ERROR:: Inproper number of arguments!"
	printFormat
	exit
fi

find $BASELINE_DIR -type f | xargs echo | xargs perl -pi -e "s/$FIND/$REPLACE/g"
