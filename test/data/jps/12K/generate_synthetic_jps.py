#!/usr/bin/python
import subprocess
import sys
import os
import re
import math
import random

# check conditions
if len(sys.argv) != 2:
	print "ERROR! Missing argument!"
	print "Invocation: ./pre-process_pavlo_torrent_peer.py FILENAME"
	sys.exit(1)

FILENAME = sys.argv[1]

# useful work
# Schema: <str, str, str, join>
def generate_input_relation(filename):
	print "Generating relation for " + filename + " ... "
	rel = []
	for i in range(0, inputsFirstSegment):
		randomFirstSegment = random.randint(0, endDomainFirstSegment)
		rel.append(randomFirstSegment)

	for i in range(0, inputsSecondSegment):
		randomSecondSegment = random.randint(beginDomainSecondSegment, endDomainSecondSegment)
		rel.append(randomSecondSegment)

	print "Shuffling the relation " + filename + " ... "
	random.shuffle(rel)


	print "Let's add some payload for " + filename + " ... "
	with open(filename, 'w') as outputFile:
		for tuple in rel:
			tuple = "BLA|HAHAHAHAAH|IHAHAHAHA|" + str(tuple) + "|\n"
			outputFile.write(tuple)

# given variables
relSize = 12000
percentageFirstSegment = 0.8333
comparisonValue = 1
weightOutput = 0.2
#weightOutput = 0.46
dispersionFirstSegment = 4.0
multiplier = 4
#multiplier = 3

# computed variables
# inputs
joinableSetSize = 1 + 2 * comparisonValue
numInputs = 2 * relSize
inputsFirstSegment = int (relSize * percentageFirstSegment)
print "Inputs first segment is " + str(inputsFirstSegment)
endDomainFirstSegment = int(inputsFirstSegment * dispersionFirstSegment)
print "EndDomainFirstSegment " +str(endDomainFirstSegment)
domainFirstSegment = range(0,endDomainFirstSegment)
inputsSecondSegment = relSize - inputsFirstSegment
print "InputsSecondSegment " + str(inputsSecondSegment)

# outputs and selectivity
numOutputs = int (numInputs / weightOutput)
print "NumOutputs is " + str(numOutputs)
outputsFirstSegment = int (joinableSetSize / dispersionFirstSegment * inputsFirstSegment)
print "Outputs from the first segment is " + str(outputsFirstSegment)
outputsSecondSegment = numOutputs - outputsFirstSegment
print "Outputs from the second segment is " + str(outputsSecondSegment)
joinSelectivitySecondSegment = int (outputsSecondSegment/inputsSecondSegment)
print "Join Selectivity second segment is " + str(joinSelectivitySecondSegment)

freqSecondSegment = multiplier * int(math.sqrt(joinSelectivitySecondSegment/joinableSetSize))
print "FreqSecondSegment is " + str(freqSecondSegment)
beginDomainSecondSegment = endDomainFirstSegment
endDomainSecondSegment = beginDomainSecondSegment + int (inputsSecondSegment / freqSecondSegment)
domainSecondSegment = range(beginDomainSecondSegment, endDomainSecondSegment)
print "domainSecondSegment is [" + str(beginDomainSecondSegment) + ", " + str(endDomainSecondSegment) + str(")")

# generate input relations
generate_input_relation(FILENAME + "_1.tbl")
generate_input_relation(FILENAME + "_2.tbl")
