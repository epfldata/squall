#!/bin/bash

./snd_storm_conf.sh $1
./reset_all.sh
./ewh_loop_cluster.py $2
