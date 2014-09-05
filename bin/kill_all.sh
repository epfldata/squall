#!/bin/bash
. ./storm_env.sh

ssh $MASTER $STORM_INSTALL_DIR/bin/kill_storm_all.sh
