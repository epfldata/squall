#!/bin/bash

# configs
CONFIG_PATH=../experiments/histogram_sigmod_2015_more/jps/different_widths/EWH_baseline/
BASELINE_DIR=$CONFIG_PATH/X4
DIRECTORY_PREFIX=X

FIND="COMPARISON_VALUE 4"
REPLACE_START="COMPARISON_VALUE"
VALUES="8,12,16,20,24,28,32"
# end of configs

for i in $(eval echo "{$VALUES}")
do
	REPLACE="$REPLACE_START $i"
	DST_DIR=${CONFIG_PATH}/${DIRECTORY_PREFIX}$i
	cp -r $BASELINE_DIR $DST_DIR
	find $DST_DIR -type f | xargs echo | xargs perl -pi -e "s/$FIND/$REPLACE/g"
done
