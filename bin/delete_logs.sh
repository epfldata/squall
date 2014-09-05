#!/bin/bash
. ./storm_env.sh

echo "Deleting Squall logs..."
# deleting logs
# TODO, we don't delete because Storm behaves strangely
ssh $MASTER echo "" '>' $STORM_LOGPATH/nimbus.log

# Removing logs from blades
# To avoid deleting /*, We check if the variables are empty or non-set
: ${STORM_LOGPATH:?"Need to set STORM_LOGPATH non-empty"}
ssh $MASTER crm -r $STORM_LOGPATH/'"*"'
