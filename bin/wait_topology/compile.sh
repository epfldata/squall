#!/bin/bash

. ../storm_version.sh

STORM_PATH=../../$STORMNAME/
STORM_LIB_PATH=$STORM_PATH/lib

javac -cp $STORM_PATH/$STORMNAME.jar:$STORM_LIB_PATH/libthrift7-0.7.0.jar topologydone/Main.java