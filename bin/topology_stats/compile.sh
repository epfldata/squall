#!/bin/bash

. ../storm_env.sh

LOCAL_STORM_PATH=../../$STORMNAME
LOCAL_STORM_LIB_PATH=$LOCAL_STORM_PATH/lib

# Could be used only for 0.8: javac -cp $LOCAL_STORM_PATH/$STORMNAME.jar:$LOCAL_STORM_LIB_PATH/libthrift7-0.7.0.jar stats/TopologyStats.java
# Could be used for 0.9: javac -cp $LOCAL_STORM_LIB_PATH/storm-core-0.9.2-incubating.jar:$LOCAL_STORM_LIB_PATH/libthrift7-0.7.0.jar:. stats/TopologyStats.java
# Could be used for 0.9:
javac -cp $LOCAL_STORM_LIB_PATH/*:. stats/TopologyStats.java
