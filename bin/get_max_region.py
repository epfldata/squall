#!/usr/bin/python
import subprocess
import sys
import os
import re

WF_OUTPUT=0.2

# functions
def get_number(filename, expr):
	# read file
	textfile = open(filename, 'r')
	filetext = textfile.read()
	textfile.close()

	# find last occurence
	matches = re.findall(expr, filetext)
	if len(matches) != 0:
		lastOccurence = matches[-1]
		number = re.findall(r'\d+', lastOccurence)[-1]
		return (lastOccurence, number)
	else:
		return ("", 0)

# Wrong old version
# Memory used: usedMemory, machineUsedMemory
def get_sum(filename, expr):
	# read file
	textfile = open(filename, 'r')
	filetext = textfile.read()
	textfile.close()

	# find last occurence
	matches = re.findall(expr, filetext)
	if len(matches) != 0:
		lastOccurence = matches[-1]
		#print "Last Memory "
		#print lastOccurence
		sumEl = 0
		for number in lastOccurence:
			sumEl += float(number)
		return (lastOccurence, sumEl)
	else:
		return ("", 0)

# Memory used: usedMemory, machineUsedMemory
def get_float(filename, expr):
	# read file
	textfile = open(filename, 'r')
	filetext = textfile.read()
	textfile.close()

	# find last occurence
	matches = re.findall(expr, filetext, re.VERBOSE)
	if len(matches) != 0:
		lastOccurence = matches[-1]
		#print "Last Memory "
		#print lastOccurence
		return (lastOccurence)
	else:
		return (0,0)

def get_weight(rinput, routput):
	return 1*float(rinput) + WF_OUTPUT*float(routput)

def get_recursive_files(expPath):
	fileList = []
	for root, subFolders, files in os.walk(expPath):
		for file in files:
			fileList.append(os.path.join(root,file))
	return fileList

# begin of main
# check conditions
if len(sys.argv) != 2:
	print "ERROR! Missing argument!"
	print "Invocation: ./get_max_region.py EXP_PATH"
	sys.exit(1)

EXP_PATH = sys.argv[1]
if os.path.isdir(EXP_PATH) == False:
	print "ERROR! %s is not a directory!" % EXP_PATH
	sys.exit(1)

# useful work
maxNumInputTuples = 0
maxNumOutputTuples = 0
maxWeight = 0
maxWeightInput = 0
maxWeightOutput = 0
maxUsedMemory = 0.0
maxAllocatedMemory = 0.0
clusterUsedMemory = 0.0
clusterAllocatedMemory = 0.0
clusterNumInputTuples = 0
clusterNumOutputTuples = 0
for filename in get_recursive_files(EXP_PATH):
	print filename

	(lastOccurence, machineNumInputTuples) = get_number(filename, "Total:,\d+,")
	print "Machine Input number of tuples is %s" % machineNumInputTuples
	clusterNumInputTuples += int(machineNumInputTuples)
	if (int(machineNumInputTuples) > int(maxNumInputTuples)):
		maxNumInputTuples = machineNumInputTuples

	(lastOccurence, machineNumOutputTuples) = get_number(filename, "Sent Tuples,\d+")
	print "Machine Output number of tuples is %s" % machineNumOutputTuples
	clusterNumOutputTuples += int(machineNumOutputTuples)
	if(int(machineNumOutputTuples) > int(maxNumOutputTuples)):
		maxNumOutputTuples = machineNumOutputTuples

	machineWeight = get_weight(machineNumInputTuples, machineNumOutputTuples)
	print "Machine Weight is %s" % machineWeight
	if(float(machineWeight) > float(maxWeight)):
		maxWeight = machineWeight
		maxWeightInput = machineNumInputTuples
		maxWeightOutput = machineNumOutputTuples

	#(machineUsedMemory,machineAllocatedMemory) = get_float(filename, "Memory used: ,(\d+\.\d+),(\d+\.\d+)")
	(machineUsedMemory,machineAllocatedMemory) = get_float(filename, "Memory\ used:\ ,( \d+ \. \d+ (?: [Ee] [+-]? \d+)? ),( \d+ \. \d+ (?: [Ee] [+-]? \d+)? )")
	print "Machine Memory is %s KBs." % machineUsedMemory
	clusterUsedMemory += float(machineUsedMemory)
	if(float(machineUsedMemory) > float(maxUsedMemory)):
		maxUsedMemory = machineUsedMemory
	print "Machine Allocated is %s KBs." % machineAllocatedMemory
	clusterAllocatedMemory += float(machineAllocatedMemory)
	if(float(machineAllocatedMemory) > float(maxAllocatedMemory)):
		maxAllocatedMemory = machineAllocatedMemory

print "Cluster statistics ..."
print "maxNumInputTuples is %s." % maxNumInputTuples
print "maxNumOutputTuples is %s." % maxNumOutputTuples
print "maxWeight is %s. Its input is %s and its output is %s." % (maxWeight, maxWeightInput, maxWeightOutput)
print "maxUsedMemory is %f GBs." % (float(maxUsedMemory)/(1024.0*1024.0))
print "maxAllocatedMemory is %f GBs." % (float(maxAllocatedMemory)/(1024.0*1024.0))
print "clusterUsedMemory is %f GBs." % (clusterUsedMemory/(1024.0*1024.0))
print "clusterAllocatedMemory is %f GBs." % (clusterAllocatedMemory/(1024.0*1024.0))
print "TotalNumOfInputs is %s (To get the size of both relations, divide it with sqrt(J))." % clusterNumInputTuples
print "   For J = 16, the size of both input relations is %s." % ((int(clusterNumInputTuples))/4)
print "TotalNumOfOutputs is %s." % clusterNumOutputTuples
