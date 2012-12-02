#!/bin/bash

INPUT_DIR=perf_input
RESULT_DIR=perf_output
EXCL_FILES=( `ls ${INPUT_DIR}/` )
COUNT=${#EXCL_FILES[@]}

declare -i i
i=1
for FILE in ${EXCL_FILES[@]} 
do
  echo "Running $i out of $COUNT ... "
  ./excl_measure.sh $INPUT_DIR/$FILE $RESULT_DIR
  i+=1
done
