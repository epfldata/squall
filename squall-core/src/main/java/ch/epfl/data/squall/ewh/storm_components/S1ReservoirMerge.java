/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.ewh.storm_components;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import ch.epfl.data.squall.ewh.data_structures.FixedSizePriorityQueue;
import ch.epfl.data.squall.ewh.data_structures.KeyPriorityProbability;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.StormEmitter;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;

public class S1ReservoirMerge<JAT extends Number & Comparable<JAT>> extends
	BaseRichBolt implements StormEmitter {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(S1ReservoirMerge.class);

    private StormEmitter _s1ReservoirGenerator;
    private final String _s1ReservoirGeneratorIndex, _componentIndex;
    private final String _componentName;
    private NumericType _wrapper;
    private ComparisonPredicate _comparison;
    private Map _conf;
    private OutputCollector _collector;

    private int _hierarchyPosition;
    private int _firstNumOfBuckets, _secondNumOfBuckets;
    private int _numRemainingParents;

    private int _outputSampleSize = -1;
    private PriorityQueue<KeyPriorityProbability> _reservoir;

    private boolean _isFirstTuple = true;
    private int _numReservoirTuplesReceived;

    public S1ReservoirMerge(StormEmitter s1ReservoirGenerator,
	    String componentName, NumericType<JAT> wrapper,
	    ComparisonPredicate comparison, int firstNumOfBuckets,
	    int secondNumOfBuckets, List<String> allCompNames,
	    int hierarchyPosition, TopologyBuilder builder,
	    TopologyKiller killer, Config conf) {

	_s1ReservoirGenerator = s1ReservoirGenerator;
	_s1ReservoirGeneratorIndex = String.valueOf(allCompNames
		.indexOf(s1ReservoirGenerator.getName()));

	_componentName = componentName;
	_componentIndex = String.valueOf(allCompNames.indexOf(componentName));

	_hierarchyPosition = hierarchyPosition;
	_conf = conf;
	_comparison = comparison;
	_wrapper = wrapper;

	_firstNumOfBuckets = firstNumOfBuckets;
	_secondNumOfBuckets = secondNumOfBuckets;

	final int parallelism = 1;

	// connecting with previous level
	InputDeclarer currentBolt = builder.setBolt(componentName, this,
		parallelism);

	currentBolt = MyUtilities.attachEmitterHash(
		SystemParameters.RESERVOIR_TO_MERGE, conf, null, currentBolt,
		_s1ReservoirGenerator);
	// acks from _s1ReservoirGenerator
	currentBolt = MyUtilities.attachEmitterHash(conf, null, currentBolt,
		_s1ReservoirGenerator);

	// manually setting reservoir size
	if (!MyUtilities.isAutoOutputSampleSize(conf)) {
	    _outputSampleSize = MyUtilities.computeManualSampleSize(conf,
		    _firstNumOfBuckets, _secondNumOfBuckets);
	    LOG.info("Manually setting outputSampleSize to "
		    + _outputSampleSize);
	    constructReservoir();
	}

	if (_hierarchyPosition == StormComponent.FINAL_COMPONENT) {
	    killer.registerComponent(this, componentName, parallelism);
	}
    }

    private void constructReservoir() {
	// LOG.info("Construction reservoir in S1ReservoirMerge of capacity " +
	// _outputSampleSize);
	_reservoir = new FixedSizePriorityQueue<KeyPriorityProbability>(
		_outputSampleSize,
		new KeyPriorityProbability.KeyPriorityComparator());
    }

    private void processNonLastTuple(String inputComponentIndex,
	    String sourceStreamId, List<String> tuple) {
	if (sourceStreamId.equals(SystemParameters.RESERVOIR_TO_MERGE)) {
	    if (MyUtilities.isOutputSampleSize(tuple)) {
		if (_reservoir == null) {
		    // first we get reservoir size; second condition is because
		    // we get this multiple times
		    _outputSampleSize = Integer.parseInt(tuple.get(1));
		    constructReservoir();
		}
	    } else {
		if (_isFirstTuple) {
		    LOG.info("First reservoir tuple received.");
		    _isFirstTuple = false;
		}
		String strKey = tuple.get(0);
		String strPriority = tuple.get(1);
		double priority = Double.parseDouble(strPriority);
		String strD2KeyProbability = tuple.get(2);
		double d2KeyProbability = Double
			.parseDouble(strD2KeyProbability);
		KeyPriorityProbability keyPriority = new KeyPriorityProbability(
			strKey, priority, d2KeyProbability);
		_reservoir.add(keyPriority);
		_numReservoirTuplesReceived++;
	    }
	} else {
	    throw new RuntimeException("In S1ReduceBolt, unrecognized source "
		    + inputComponentIndex);
	}
    }

    private void finalizeProcessing() {
	// we are only sending S1 tuples (join keys from R1)
	LOG.info("I received a total reservoir tuples from S1ReservoirGenerator "
		+ _numReservoirTuplesReceived
		+ ".\n"
		+ "This is the sum of all the reservoir tuples received from S1ReservoirGenerator "
		+ "(should be (at most) J * outputSampleSize large).");
	LOG.info("Started finalize processing, which is sending reservoir tuples to D2Combiner.");
	if (!SystemParameters.getBooleanIfExist(_conf, "IS_WR")) {
	    KeyPriorityProbability kp;
	    while ((kp = _reservoir.poll()) != null) {
		String key = kp.getKey();
		sendKey(key);
	    }
	} else {
	    long startTime = System.currentTimeMillis();

	    // transform priority queue to an ordinary array,
	    // which is a simulation of Partial Sum Trees from WE80 from [Simple
	    // Random Sampling from Relational Databases, VLDB 1986]
	    ArrayList<KeyPriorityProbability> keyProbabilities = new ArrayList<KeyPriorityProbability>();
	    KeyPriorityProbability kp;
	    while ((kp = _reservoir.poll()) != null) {
		keyProbabilities.add(kp);
	    }

	    // sort and put duplicates together
	    // Collections.sort(keyProbabilities, new
	    // KeyPriorityProbability.D2KeyProbabilityComparator());
	    // TODO optimization: put duplicates together. Seems not important
	    // as the S1 sample size is very small (~10.000 elements)

	    // add probabilities of all before me
	    double totalProbability = 0;
	    for (KeyPriorityProbability kpp : keyProbabilities) {
		double currentProbability = kpp.getD2KeyProbability();
		// the probability of the first element is zero (its lower
		// probability)
		kpp.setD2KeyProbability(totalProbability);
		totalProbability += currentProbability;
	    }

	    // totalProbability is the sum of
	    // D2Probabilities(d2Mult/d2RelationSize)
	    // corresponding to S1 tuples
	    // 0 < totalProbability < 1
	    // is much higher than keyProbabilities.size/d2RelationSize,
	    // due to the fact that the algorithm for choosing S1 prefers high
	    // d2Mult
	    LOG.info("WR: S1 reservoir size is " + keyProbabilities.size()
		    + ". Sending further down " + _outputSampleSize
		    + " tuples.");

	    // randomly choose from Partial Sum Trees, and send it further down
	    Random rndGen = new Random();
	    // for(int i = 0; i < keyProbabilities.size(); i++){
	    // commented out because keyProbabilities can be smaller than
	    // _outputSampleSize
	    for (int i = 0; i < _outputSampleSize; i++) {
		double random = rndGen.nextDouble() * totalProbability; // simulate
									// [0,
									// totalProbability)
									// range
		int index = binarySearch(keyProbabilities, random,
			totalProbability);
		String key = keyProbabilities.get(index).getKey();
		sendKey(key);
	    }

	    double elapsed = (System.currentTimeMillis() - startTime) / 1000.0;
	    LOG.info("WoR to WR takes " + elapsed + " seconds.");
	}
	LOG.info("Ended finalize processing, which is sending reservoir tuples to D2Combiner.");
    }

    /*
     * arg: KeyPriorityProbability contains the prefix sum of probabilities
     * before current position: arg: probability is [0, 1) returns position in
     * the keyProbabilities
     */
    private int binarySearch(List<KeyPriorityProbability> keyProbabilities,
	    double probability, double totalProbability) {
	int leftBoundary = 0;
	int rightBoundary = keyProbabilities.size() - 1;

	while (leftBoundary <= rightBoundary) {
	    int currentPosition = (leftBoundary + rightBoundary) / 2;
	    double currentLowerProb = keyProbabilities.get(currentPosition)
		    .getD2KeyProbability();
	    double currentUpperProb = totalProbability; // for the last position
	    if (currentPosition != keyProbabilities.size() - 1) {
		// not the last one
		currentUpperProb = keyProbabilities.get(currentPosition + 1)
			.getD2KeyProbability();
	    }

	    if (probability < currentLowerProb) {
		rightBoundary = currentPosition - 1;
	    } else if (probability >= currentUpperProb) {
		leftBoundary = currentPosition + 1;
	    } else {
		// we found it!
		return currentPosition;
	    }
	}

	throw new RuntimeException("Binary search problem. leftBoundary = "
		+ leftBoundary
		+ ", rightBoundary = "
		+ rightBoundary
		+ ", asked for probability = "
		+ probability
		+ ", lastElementLowerProb = "
		+ keyProbabilities.get(keyProbabilities.size() - 1)
			.getD2KeyProbability());
    }

    private void sendKey(String key) {
	List<String> tuple = new ArrayList<String>(Arrays.asList(key));
	List<Integer> hashIndexes = new ArrayList<Integer>(Arrays.asList(0));
	tupleSend(tuple, hashIndexes);
	if (SystemParameters.getBooleanIfExist(_conf, "DEBUG_MODE")) {
	    // this is just for local debugging
	    LOG.info("Reservoir element in S1ReservoirMerge is " + tuple);
	}
    }

    // BaseRichSpout
    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
	_collector = collector;
	_numRemainingParents = MyUtilities.getNumParentTasks(tc,
		Arrays.asList(_s1ReservoirGenerator));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	if (_hierarchyPosition == StormComponent.FINAL_COMPONENT) {
	    declarer.declareStream(SystemParameters.EOF_STREAM, new Fields(
		    SystemParameters.EOF));
	} else {
	    final List<String> outputFields = new ArrayList<String>();
	    outputFields.add(StormComponent.COMP_INDEX);
	    outputFields.add(StormComponent.TUPLE); // list of string
	    outputFields.add(StormComponent.HASH);
	    declarer.declareStream(SystemParameters.DATA_STREAM, new Fields(
		    outputFields));
	}
    }

    // ----------- below you don't need to change --------------
    // if true, we should exit from method which called this method
    @Override
    public void execute(Tuple stormTupleRcv) {
	final String inputComponentIndex = stormTupleRcv
		.getStringByField(StormComponent.COMP_INDEX); // getString(0);
	final List<String> tuple = (List<String>) stormTupleRcv
		.getValueByField(StormComponent.TUPLE); // getValue(1);
	String sourceStreamId = stormTupleRcv.getSourceStreamId();

	if (processFinalAck(tuple, stormTupleRcv))
	    return;

	processNonLastTuple(inputComponentIndex, sourceStreamId, tuple);

	_collector.ack(stormTupleRcv);
    }

    private void tupleSend(List<String> tuple, List<Integer> hashIndexes) {
	final Values stormTupleSnd = MyUtilities.createTupleValues(tuple, 0,
		_componentIndex, hashIndexes, null, _conf);
	MyUtilities.sendTuple(stormTupleSnd, null, _collector, _conf);
    }

    private void tupleSend(String streamId, List<String> tuple,
	    List<Integer> hashIndexes) {
	final Values stormTupleSnd = MyUtilities.createTupleValues(tuple, 0,
		_componentIndex, hashIndexes, null, _conf);
	MyUtilities.sendTuple(streamId, stormTupleSnd, null, _collector, _conf);
    }

    protected boolean processFinalAck(List<String> tuple, Tuple stormTupleRcv) {
	if (MyUtilities.isFinalAck(tuple, _conf)) {
	    _numRemainingParents--;
	    if (_numRemainingParents == 0) {
		finalizeProcessing();
	    }
	    if (_numRemainingParents < 0) {
		throw new RuntimeException(
			"Negative number of remaining parents: "
				+ _numRemainingParents);
	    }
	    MyUtilities.processFinalAck(_numRemainingParents,
		    _hierarchyPosition, _conf, stormTupleRcv, _collector);
	    return true;
	}
	return false;
    }

    // from IRichBolt
    @Override
    public Map<String, Object> getComponentConfiguration() {
	return _conf;
    }

    @Override
    public String[] getEmitterIDs() {
	return new String[] { _componentName };
    }

    @Override
    public String getName() {
	return _componentName;
    }

    @Override
    public String getInfoID() {
	throw new RuntimeException("Should not be here!");
    }
}