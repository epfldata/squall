#!/bin/sh

# DBTOASTER_HOME=~/opt/dbtoaster
. ./storm_env.sh
DBTOASTER_PKG=../contrib/dbtoaster/dbtoasteralpha5-release.tar.gz

if [ ! -f "$DBTOASTER_HOME/bin/dbtoaster" ]; then
    mkdir -p $DBTOASTER_HOME
    echo "Extracting $DBTOASTER_PKG to $DBTOASTER_HOME"
    tar -xzf $DBTOASTER_PKG -C $DBTOASTER_HOME --strip-components=1
    chmod +x $DBTOASTER_HOME/bin/dbtoaster
else 
    echo "DBToaster is already installed. Skip installation"
fi

echo "Set DBTOASTER_HOME to $DBTOASTER_HOME"
export DBTOASTER_HOME=$DBTOASTER_HOME
