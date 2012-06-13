#!/bin/bash
if [ $# -ne 2 ]; then
	echo "Illegal number of command line arguments. Expected 2, got $#"
	echo "Usage: ./create_ramdisk.sh <MOUNT_POINT> <SIZE IN MB>"
	exit
fi

MOUNT_DIR=$1
if [ ! -d $MOUNT_DIR ]; then
	echo "Mount directory \"$MOUNT_DIR\" does not exist. Exiting..."
	exit
fi
SIZE_MB=$2

mkdir -p $MOUNT_DIR
sudo mount -t tmpfs -o size=${SIZE_MB}M tmpfs /tmp/ramdisk/
