#!/usr/bin/python
import subprocess
import sys
import os

if len(sys.argv) != 2:
	print "ERROR! Missing argument!"
	print "Invocation: ./ewh_loop_cluster.py RUN_PATH"
	sys.exit(1)

RUN_PATH = sys.argv[1]
if os.path.isdir(RUN_PATH) == False:
	print "ERROR! %s is not a directory!" % RUN_PATH
	sys.exit(1)

subdirs = os.walk(RUN_PATH).next()[1]
subdirs.sort()
for subdir in subdirs:
	if subdir != "exclude":
		fullSquallPath = RUN_PATH + "/" + subdir
		print "Running loop_squall_cluster on %s" % fullSquallPath
		#./loop_squall_cluster.sh <MODE> <PROFILING> <RESTART_BEFORE> <RESTART_AFTER_EACH> <GET_KEY_REGIONS> <BASE_PATH>"
		subprocess.call("./loop_squall_cluster.sh PLAN_RUNNER NO NO NO YES %s" % fullSquallPath, shell=True)
