#!/bin/bash

BASE_DIR=extract_constants/SquallJoins
INPUT_DIR=$BASE_DIR/cluster
RESULT_DIR=$BASE_DIR/output
CONF_FILES=( `ls ${INPUT_DIR}/` )
COUNT=${#CONF_FILES[@]}

mkdir -p $RESULT_DIR
./recompile.sh

declare -i i
i=1
for FILE in ${CONF_FILES[@]} 
do
  echo "Running $i out of $COUNT ... "
  ./manual_batch.sh $INPUT_DIR/$FILE $RESULT_DIR
  i+=1
done
