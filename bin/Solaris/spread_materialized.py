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

def ssh_command_err(host, command):
	ssh = subprocess.Popen(["ssh", "%s" % host, command],
	                       shell=False,
	                       stdout=subprocess.PIPE,
	                       stderr=subprocess.PIPE)
	result = ssh.stdout.readlines()
	if result == []:
		error = ssh.stderr.readlines()
		print >>sys.stderr, "ERROR: %s" % error
		return result
	else:
		return result

	# Alternative with a library
	# ssh = paramiko.SSHClient()
	# ssh.connect(server, username=username, password=password)
	# ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd_to_execute)

	# Alternative with no library
	# import subprocess
	# subprocess.check_call(['ssh', 'server', 'command'])
	# subprocess.check_call(['scp', 'server:file', 'file'])


def scp_command_err(srcMachine, dstMachine, path, filename):
	srcPath = srcMachine + ":" + path + "/" + filename
	dstPath = dstMachine + ":" + path
	print "Copying from", srcPath, "to", dstPath
	p = subprocess.Popen(["scp", "%s" % srcPath, dstPath],
	                       shell=False,
	                       stdout=subprocess.PIPE,
	                       stderr=subprocess.PIPE)
   error = p.stderr.readlines()
	if error != []:
		print >>sys.stderr, "ERROR: %s" % error
	sts = os.waitpid(p.pid, 0)



PATH="/data/squall_blade/data/tpchdb/Skewed/10G/"
FILENAMES=["tpch7_l.tbl", "tpch7_sn.tbl"]
RANGE=range(5,11)
MACHINE="squalldata@icdatasrv"

for filename in FILENAMES:
	FULL_PATH=PATH + "/" + filename

	# find file
	CONTAINS_COMMAND="ls -l " + FULL_PATH
	for blade in RANGE:
		host = MACHINE + "%s" % blade
		contains = ssh_command(host, CONTAINS_COMMAND)
		if contains != []:
			hostIndexFile = blade
			break;
	# print machine containing of the file
	srcMachine = MACHINE + "%s" % hostIndexFile
	print "File ", filename, " is on", srcMachine

	# copy file to all other machines
	for blade in RANGE:
		dstMachine = MACHINE + "%s" % blade
		if blade != hostIndexFile:
			scp_command_err(srcMachine, dstMachine, PATH, filename)
