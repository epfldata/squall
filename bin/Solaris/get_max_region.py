#!/usr/bin/python
import subprocess
import sys
import os
import re

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

def get_weight(rinput, routput):
	return 1*float(rinput) + 0.46*float(routput)

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
maxInput = 0
maxOutput = 0
maxWeight = 0
maxMemory = 0
for filename in get_recursive_files(EXP_PATH):
	print filename

	(lastOccurence, totalInput) = get_number(filename, "Total:,\d+,")
	print "TotalInput is %s" % totalInput
	if (int(totalInput) > int(maxInput)):
		maxInput = totalInput

	(lastOccurence, totalOutput) = get_number(filename, "Sent Tuples,\d+")
	print "TotalOutput is %s" % totalOutput
	if(int(totalOutput) > int(maxOutput)):
		maxOutput = totalOutput

	weight = get_weight(totalInput, totalOutput)
	print "TotalWeight is %s" % weight
	if(float(weight) > float(maxWeight)):
		maxWeight = weight

	(lastOccurence, totalMemory) = get_sum(filename, "Memory used: ,(\d+\.\d+),(-?\d+\.\d+)")
	print "TotalMemory is %s" % totalMemory
	if(float(totalMemory) > float(maxMemory)):
		maxMemory = totalMemory

print "MaxInput is %s" % maxInput
print "MaxOutput is %s" % maxOutput
print "MaxWeight is %s" % maxWeight
print "MaxMemory is %s" % maxMemory
