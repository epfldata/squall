#!/bin/bash
. ./storm_version.sh

# Arguments are OPERATION RESTART_ANYWAY
#               START/END YES/NO

MACHINE=squalldata@icdatasrv5
CLUSTER_NODE_CONF=/opt/storm/$STORMNAME/conf/storm.yaml

declare -i CLUSTER_MODE

# Returns 1 if cluster is in profiling mode, 0 otherwise
check_cluster_mode() {
	scp ${MACHINE}:${CLUSTER_NODE_CONF} storm.yaml.tmp
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
	./snd_conf.sh storm.yaml.profiling
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
	./snd_conf.sh storm.yaml
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
