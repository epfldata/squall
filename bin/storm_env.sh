#!/bin/bash

# MASTER is visible from the outside world, and runs C3Tools
MASTER=datalab@icdataportal3
# This is the format that C3Tools expects
BLADES=1-10

STORM_INSTALL_DIR=/data/lab/fromhome/avitorovic/storm
STORMNAME=storm-0.9.2-incubating
#STORMNAME=apache-storm-0.9.4
STORMPATH=$STORM_INSTALL_DIR/$STORMNAME

STORM_TMP_DIR=/data/lab/storm_tmp
STORM_KILL_TMP_DIR=/data/lab/storm_tmp/temp
STORM_LOGPATH=$STORM_TMP_DIR/logs
STORM_SNAPSHOTPATH=$STORM_TMP_DIR/profiling
STORM_DATA=$STORM_TMP_DIR/storm_data
ZOOKEEPERPATH=$STORM_TMP_DIR/zookeeper_data

# Used for storing .storm directory
CLUSTER_HOME=/data/lab/

# Gathering stuff from the cluster (first directory is always per blade, and the second is per master)
CLUSTER_KEYPATH=$STORM_TMP_DIR/key_region
CLUSTER_GATHER_KEYS=/data/lab/fromhome/avitorovic/gather_keys

CLUSTER_R2HIST=$STORM_TMP_DIR/r2_hist
CLUSTER_GATHER_HIST=/data/lab/fromhome/avitorovic/gather_hist

CLUSTER_GATHER_DIR=/data/lab/fromhome/avitorovic/gather_logs
CLUSTER_GATHER_SNAPSHOT_DIR=/data/lab/fromhome/avitorovic/gather_snapshots

# DBTOASTER installation folder. Only required if QueryPlan uses DBToasterJoinComponent
DBTOASTER_HOME=~/opt/dbtoaster
