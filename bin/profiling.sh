#!/bin/bash
. ./storm_env.sh

# Arguments are OPERATION RESTART_ANYWAY
#               START/END YES/NO

CLUSTER_NODE_CONF=$STORM_INSTALL_DIR/$STORMNAME/conf/storm.yaml

declare -i CLUSTER_MODE

# Returns 1 if cluster is in profiling mode, 0 otherwise
check_cluster_mode() {
	scp $MASTER:$CLUSTER_NODE_CONF storm.yaml.tmp
	CLUSTER_MODE=`cat storm.yaml.tmp | grep "agentpath" | wc -l`
	rm storm.yaml.tmp
	return $CLUSTER_MODE	
}

restart_cluster() {
	echo "Restarting cluster... "
	./reset_all.sh;
	echo "DONE!"
}

init_cluster_profiling() {
	check_cluster_mode
	if [ $? == 1 ]; then
		if [ "$1" == "YES" ]; then
			restart_cluster
		fi
		return # Cluster already in profiling mode
	fi
	echo "Updating cluster with profiling configuration..."
	./snd_storm_conf.sh ../resources/storm-linux_4GB_profiling.yaml
	echo "DONE"
	restart_cluster
}

restore_normal_cluster() {
	check_cluster_mode
	if [ $? == 0 ]; then
		if [ "$1" == "YES" ]; then
			restart_cluster
		fi
		return # Cluster already in normal mode
	fi
	echo "Updating cluster with normal configuration..."
   # NOTE: No guarantee that storm.yaml is the current configuration; it's fine as long as we are not in profile mode
	./snd_storm_conf.sh ../resources/storm-linux_4GB.yaml
	echo "DONE"
	restart_cluster
}

if [ "$1" == "START" ]; then
	init_cluster_profiling $2
elif [ "$1" == "END" ]; then
	restore_normal_cluster $2
else
	echo "Undefined command '$1' received. Exiting..."
	exit
fi
