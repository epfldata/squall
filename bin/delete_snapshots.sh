#!/bin/bash
. ./storm_env.sh

echo "Deleting profiling snapshots..."
# deleting snapshots
ssh $MASTER rm -r $STORM_SNAPSHOTPATH/"*"

# Removing snapshots from blades
# To avoid deleting /*, We check if the variables are empty or non-set
: ${STORM_SNAPSHOTPATH:?"Need to set STORM_SNAPSHOTPATH non-empty"}
ssh $MASTER crm -r $STORM_SNAPSHOTPATH/'"*"'
