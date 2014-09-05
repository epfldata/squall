#!/bin/bash
. ./storm_env.sh

# Deleting all the Storm output on Master and blades
# To avoid deleting /*, We check if the variables are empty or non-set
: ${STORM_LOGPATH:?"Need to set STORM_LOGPATH non-empty"}
ssh $MASTER rm -r $STORM_LOGPATH/*
ssh $MASTER crm -r $STORM_LOGPATH/'"*"'

: ${STORM_DATA:?"Need to set STORM_DATA non-empty"}
ssh $MASTER rm -r $STORM_DATA/*
ssh $MASTER crm -r $STORM_DATA/'"*"'

: ${ZOOKEEPERPATH:?"Need to set ZOOKEEPERPATH non-empty"}
ssh $MASTER rm -r $ZOOKEEPERPATH/*
ssh $MASTER crm -r $ZOOKEEPERPATH/'"*"'
