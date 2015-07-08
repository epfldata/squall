#!/bin/bash

# DBTOASTER_HOME=~/opt/dbtoaster
. ./storm_env.sh
DBTOASTER_PKG=../contrib/dbtoaster/dbtoaster-alpha5-release.tar.gz
DBTOASTER_FRONTENDS=../contrib/dbtoaster/front_ends

if [ ! -f "$DBTOASTER_HOME/bin/dbtoaster" ]; then
    echo "Installing DBToaster to $DBTOASTER_HOME"
    # select the correct binary version of dbtoaster_frontend 
    if [ "$(uname)" == "Darwin" ]; then
        DBT_FRONTEND=$DBTOASTER_FRONTENDS/dbtoaster_frontend_macosx
    elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
        if [ "$(uname -p)" == "x86_64" ]; then
            DBT_FRONTEND=$DBTOASTER_FRONTENDS/dbtoaster_frontend_linux_x86-64
        else 
            DBT_FRONTEND=$DBTOASTER_FRONTENDS/dbtoaster_frontend_linux_x86-32
        fi
    elif [ "$(expr substr $(uname -s) 1 10)" == "MINGW32_NT" ]; then
        echo "Window platform is not supported"
        exit
    fi

    mkdir -p $DBTOASTER_HOME
    echo "Extracting $DBTOASTER_PKG to $DBTOASTER_HOME"
    tar -xzf $DBTOASTER_PKG -C $DBTOASTER_HOME --strip-components=1
    chmod +x $DBTOASTER_HOME/bin/dbtoaster
    echo "Select frontend binary $DBT_FRONTEND"
    echo "copy $DBT_FRONTEND to $DBTOASTER_HOME/bin/dbtoaster_frontend"
    cp $DBT_FRONTEND $DBTOASTER_HOME/bin/dbtoaster_frontend
else 
    echo "DBToaster is already installed at $DBTOASTER_HOME. Skip installation"
fi

echo "Set DBTOASTER_HOME to $DBTOASTER_HOME"
export DBTOASTER_HOME=$DBTOASTER_HOME
