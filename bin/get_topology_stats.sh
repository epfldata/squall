#!/bin/bash
# Similar to rake_files/extract_time.rb, but in addition it computes Storm latency (with Ackers) and throughput

. ./storm_env.sh

LOCAL_STORM_PATH=../$STORMNAME/
LOCAL_STORM_LIB_PATH=$LOCAL_STORM_PATH/lib
EXEC_DIR=topology_stats

cd $EXEC_DIR
./compile.sh
cd ..

# Could be used only for Storm 0.8
#java -cp $LOCAL_STORM_PATH/$STORMNAME.jar:$LOCAL_STORM_LIB_PATH/libthrift7-0.7.0.jar:$LOCAL_STORM_LIB_PATH/log4j-1.2.16.jar:$LOCAL_STORM_LIB_PATH/slf4j-api-1.5.8.jar:$LOCAL_STORM_LIB_PATH/slf4j-log4j12-1.5.8.jar:$LOCAL_STORM_LIB_PATH/commons-lang-2.5.jar:$EXEC_DIR/. stats.TopologyStats

# Could be used only for Storm 0.9
java -cp $LOCAL_STORM_LIB_PATH/*:$EXEC_DIR/. stats.TopologyStats
