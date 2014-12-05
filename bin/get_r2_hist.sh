#!/bin/bash
. ./storm_env.sh

# Local parameters
LOCAL_R2HIST=../test/m_bucket/r2_hist
LOCAL_R2HIST_TMP=$LOCAL_R2HIST/temp
CLUSTER_R2HIST=$STORM_TMP_DIR/r2_hist
CLUSTER_GATHER_HIST=/localhome/datalab/avitorovic/gather_hist

# DOWNALOADING
# gather to master
ssh $MASTER mkdir -p $CLUSTER_GATHER_HIST
ssh $MASTER cget $CLUSTER_R2HIST $CLUSTER_GATHER_HIST

# copy from master to local
mkdir -p $LOCAL_R2HIST_TMP
scp -r $MASTER:$CLUSTER_GATHER_HIST $LOCAL_R2HIST_TMP

# find r2 equi-depth histograms in the downloaded directory; copy it to $LOCAL_R2HIST
ARRAY=(`find $LOCAL_R2HIST_TMP -type f`)
ARRAYSIZE="${#ARRAY[@]}"
if [ $ARRAYSIZE -gt 1 ]; then
   echo "ERROR! ERROR! ERROR!: There were multiple files in $CLUSTER_R2HIST."
fi

if [ $ARRAYSIZE -gt 0 ]; then
	for rshistfile in "${ARRAY[@]}"
	do
		R2HIST=${rshistfile}
		cp $R2HIST $LOCAL_R2HIST
		if [ $# -eq 1 ]; then
			LOCAL_EXTRA_PATH=$1
			cp -r $R2HIST $LOCAL_EXTRA_PATH
		fi
		echo "Copied $R2HIST to $LOCAL_R2HIST."
	done
else
   echo "No r2_hists were produced."
fi

# CLEARING
# clear master from the gather
: ${CLUSTER_GATHER_HIST:?"Need to set CLUSTER_GATHER_HIST non-empty"}
ssh $MASTER rm -r $CLUSTER_GATHER_HIST/*

# clear from all the blades
: ${CLUSTER_R2HIST:?"Need to set CLUSTER_R2HIST non-empty"}
ssh $MASTER crm -r $CLUSTER_R2HIST/'"*"'

# clear local tmp
: ${LOCAL_R2HIST_TMP:?"Need to set LOCAL_R2HIST_TMP non-empty"}
rm -r $LOCAL_R2HIST_TMP/*
