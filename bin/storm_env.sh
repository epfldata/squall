#!/bin/bash

# MASTER is visible from the outside world, and runs C3Tools
MASTER=datalab@icdataportal3
# This is the format that C3Tools expects
BLADES=1-10

STORM_INSTALL_DIR=/localhome/datalab/avitorovic/storm
STORMNAME=storm-0.9.2-incubating
STORMPATH=$STORM_INSTALL_DIR/$STORMNAME

STORM_TMP_DIR=/data/lab/storm_tmp
STORM_KILL_TMP_DIR=/data/lab/storm_tmp/temp
STORM_LOGPATH=$STORM_TMP_DIR/logs
STORM_DATA=$STORM_TMP_DIR/storm_data
ZOOKEEPERPATH=$STORM_TMP_DIR/zookeeper_data
