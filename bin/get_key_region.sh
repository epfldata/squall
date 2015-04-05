#!/bin/bash
. ./storm_env.sh

# Local parameters
LOCAL_KEYPATH=../test/m_bucket/key_region
LOCAL_KEYPATH_TMP=$LOCAL_KEYPATH/temp

# DOWNALOADING
# gather to master
ssh $MASTER mkdir -p $CLUSTER_GATHER_KEYS
ssh $MASTER cget $CLUSTER_KEYPATH $CLUSTER_GATHER_KEYS

# copy from master to local
mkdir -p $LOCAL_KEYPATH_TMP
scp -r $MASTER:$CLUSTER_GATHER_KEYS $LOCAL_KEYPATH_TMP

# find key-regions in the downloaded directory; copy it to $LOCAL_KEYPATH
ARRAY=(`find $LOCAL_KEYPATH_TMP -type f`)
ARRAYSIZE="${#ARRAY[@]}"
if [ $ARRAYSIZE -gt 1 ]; then
   echo "WARNING: There were multiple files in $CLUSTER_KEYPATH."
   echo "   This is fine if a single configuration generates multiple keyRegions (like MBucket currently does)."
   echo "   Otherwise, make sure you clean locally $LOCAL_KEYPATH; on master $CLUSTER_GATHER_KEYS; and on blades $CLUSTER_KEYPATH."
   echo "END OF WARNING."
fi

if [ $ARRAYSIZE -gt 0 ]; then
	for keyRegion in "${ARRAY[@]}"
	do
		KEYPATH=${keyRegion}
		cp $KEYPATH $LOCAL_KEYPATH
		if [ $# -eq 1 ]; then
			LOCAL_EXTRA_PATH=$1
			cp -r $KEYPATH $LOCAL_EXTRA_PATH
		fi
		echo "Copied $KEYPATH to $LOCAL_KEYPATH."
	done
else
   echo "No key_regions were produced (e.g. 1Bucket operator was run)."
fi

# CLEARING
# clear master from the gather
: ${CLUSTER_GATHER_KEYS:?"Need to set CLUSTER_GATHER_KEYS non-empty"}
ssh $MASTER rm -r $CLUSTER_GATHER_KEYS/*

# clear from all the blades
: ${CLUSTER_KEYPATH:?"Need to set CLUSTER_KEYPATH non-empty"}
ssh $MASTER crm -r $CLUSTER_KEYPATH/'"*"'

# clear local tmp
: ${LOCAL_KEYPATH_TMP:?"Need to set LOCAL_KEYPATH_TMP non-empty"}
rm -r $LOCAL_KEYPATH_TMP/*
