#!/usr/bin/python
import subprocess
import sys
import os

def ssh_command(host, command):
	ssh = subprocess.Popen(["ssh", "%s" % host, command],
	                       shell=False,
	                       stdout=subprocess.PIPE,
	                       stderr=subprocess.PIPE)
	result = ssh.stdout.readlines()
	return result

def scp_command_err(srcMachine, srcPath, dstPath):
	srcPath = srcMachine + ":" + srcPath
	print "Copying from", srcPath, "to", dstPath
	p = subprocess.Popen(["scp", "-r", "%s" % srcPath, dstPath],
	                       shell=False,
	                       stdout=subprocess.PIPE,
	                       stderr=subprocess.PIPE)
	error = p.stderr.readlines()
	if error != []:
		print >>sys.stderr, "ERROR: %s" % error
	sts = os.waitpid(p.pid, 0)


CLUSTER_PATH="/data/squall_blade/data/tpchdb/key_region/*"
LOCAL_PATH="../test/m_bucket/key_region"
LOCAL_EXTRA_PATH=""
if len(sys.argv) == 2:
	LOCAL_EXTRA_PATH=sys.argv[1]

RANGE=range(1,11)
MACHINE="squalldata@icdatasrv"

# Format: queryName_lastTwoDataPathDirs_alg_xj_xb
# Example: theta_lines_self_join_tpchdb_10G_oco_64j_250b

# let's just copy everything 
for blade in RANGE:
	srcMachine = MACHINE + "%s" % blade
	scp_command_err(srcMachine, CLUSTER_PATH, LOCAL_PATH)
	if LOCAL_EXTRA_PATH != "":
		scp_command_err(srcMachine, CLUSTER_PATH, LOCAL_EXTRA_PATH)

# delete it afterwards: necessary to avoid grasping old keyRegion files
keyPath = CLUSTER_PATH
delete_command = "rm -rf " + keyPath
for blade in RANGE:
	host = MACHINE + "%s" % blade
	delete_result = ssh_command(host, delete_command)
	if delete_result != []:
		print delete_result
