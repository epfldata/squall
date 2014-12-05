#!/bin/bash

MACHINE=squalldata@icdatasrv
MACHINE5=squalldata@icdatasrv5
STORAGE_REMOTE_PATH=/data/squall_zone/storage
LOGS_REMOTE_PATH=/data/squall_zone/logs

# TODO Uncomment once you start dealing with BDB again
echo "NOT Deleting BDB storage..."
# deleting BDB storage
# for blade in {1..10}
# do
#   for port in {1001..1022}
#   do
# 	ssh -p "$port" $MACHINE${blade} 'rm -r ' $STORAGE_REMOTE_PATH'/*'  
#   done
# done

echo "Deleting Squall logs..."
# deleting logs
# TODO, we don't delete because Storm behaves strangely
ssh $MACHINE5 'echo "" > ' $LOGS_REMOTE_PATH'/nimbus.log'

for blade in {1..10}
do
  for port in {1001..1022}
  do
	ssh -p "$port" $MACHINE${blade} 'rm -r ' $LOGS_REMOTE_PATH'/*'
  done
done
