#!/bin/bash

DOWN_TIMEOUT=30 # to ensure all processes are killed
UP_TIMEOUT=10 # to ensure all processes are up and running

./kill_all.sh
sleep $DOWN_TIMEOUT # this is not necessary when kill_all.sh is called
./delete_outputs.sh
./run_storm_cluster.sh
sleep $UP_TIMEOUT