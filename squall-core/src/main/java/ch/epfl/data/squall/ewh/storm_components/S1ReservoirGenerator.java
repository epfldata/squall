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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.TreeMap;

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
import ch.epfl.data.squall.utilities.SystemParameters.HistogramType;

public class S1ReservoirGenerator<JAT extends Number & Comparable<JAT>> extends
	BaseRichBolt implements StormEmitter {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(S1ReservoirGenerator.class);

    private StormEmitter _d2Combiner, _s1Source;
    private final String _d2CombinerIndex, _s1SourceIndex, _componentIndex,
	    _partitionerIndex;
    private final String _componentName;
    private NumericType _wrapper;
    private ComparisonPredicate _comparison;
    private Map _conf;
    private OutputCollector _collector;

    private boolean _isEWHS1Histogram; // receives either d2 or d2equi from
				       // D2Combiner
    private static final boolean _isSinglePassD2Equi = true; // Opt2: can be set
							     // to true only for
							     // _isEWHS1Histogram
							     // = true
							     // reduces number
							     // of accesses to
							     // d2 by computing
							     // d2 from d2_equi
							     // in a single pass
							     // if turned off,
							     // check
							     // "If Opt2 is not used"

    private static final boolean _isSinglePassDebug = false; // can be set to
							     // true only if
							     // _isSinglePassD2Equi
							     // is true;
							     // checks whether
							     // the result is
							     // correct by using
							     // more space:
							     // don't use it in
							     // production (only
							     // for testing
							     // purposes)
    private Map<JAT, Integer> _d2OldDebug = new HashMap<JAT, Integer>(); // key,
									 // multiplicity:
									 // only
									 // if
									 // _isSinglePassDebug
									 // =
									 // true
    // private TreeMap<JAT, Integer> _d2Equi = new TreeMap<JAT, Integer>(); //
    // key, multiplicity: used only when _isSinglePassD2Equi is set
    private TreeMap<JAT, Integer> _d2BeginEnd = new TreeMap<JAT, Integer>(); // key,
									     // multiplicity:
									     // used
									     // only
									     // when
									     // _isSinglePassD2Equi
									     // is
									     // set
    private int _band;

    private int _hierarchyPosition;
    private int _firstNumOfBuckets, _secondNumOfBuckets;
    private int _numRemainingParents;

    private long _computedTotalOutputs;

    private TreeMap<JAT, Integer> _d2 = new TreeMap<JAT, Integer>(); // Opt1 is
								     // JAT
								     // instead
								     // of
								     // String
								     // key
								     // (key,
								     // multiplicity).
								     // If Opt2
								     // is not
								     // used,
								     // use
								     // HashMap
    private Map<String, Integer> _r1 = new HashMap<String, Integer>(); // key,
								       // multiplicity
    private PriorityQueue<KeyPriorityProbability> _reservoir;

    private Random _rndGen = new Random();

    public S1ReservoirGenerator(
	    StormEmitter d2Reduce,
	    StormEmitter s1Source,
	    String componentName,
	    String partitionerName,
	    boolean isEWHS1Histogram, // receives either d2 or d2equi from
				      // D2Combiner
	    NumericType<JAT> wrapper, ComparisonPredicate comparison,
	    int firstNumOfBuckets, int secondNumOfBuckets,
	    List<String> allCompNames, int hierarchyPosition,
	    TopologyBuilder builder, TopologyKiller killer, Config conf) {

	_d2Combiner = d2Reduce;
	_d2CombinerIndex = String.valueOf(allCompNames.indexOf(d2Reduce
		.getName()));
	_s1Source = s1Source;
	_s1SourceIndex = String
		.valueOf(allCompNames.indexOf(s1Source.getName()));
	_partitionerIndex = String.valueOf(allCompNames
		.indexOf(partitionerName));

	_componentName = componentName;
	_componentIndex = String.valueOf(allCompNames.indexOf(componentName));

	_hierarchyPosition = hierarchyPosition;
	_conf = conf;
	_comparison = comparison;
	_wrapper = wrapper;

	_isEWHS1Histogram = isEWHS1Histogram;
	_firstNumOfBuckets = firstNumOfBuckets;
	_secondNumOfBuckets = secondNumOfBuckets;

	_band = _comparison.getInclusiveDiff();

	final int parallelism = SystemParameters.getInt(conf, componentName
		+ "_PAR");

	// connecting with previous level
	InputDeclarer currentBolt = builder.setBolt(componentName, this,
		parallelism);

	if (!_isEWHS1Histogram) {
	    currentBolt = MyUtilities.attachEmitterHash(
		    SystemParameters.D2_TO_S1_STREAM, conf, null, currentBolt,
		    d2Reduce);
	    // acks from d2Reduce and all the data from s1Source
	    currentBolt = MyUtilities.attachEmitterHash(conf, null,
		    currentBolt, d2Reduce, s1Source);
	} else {
	    HistogramType dstHistType = HistogramType.S1_RES_HIST;
	    // if this histogram exists, the other must exist as well
	    if (!SystemParameters.getBooleanIfExist(_conf,
		    HistogramType.D2_COMB_HIST.readConfEntryName())) {
		throw new RuntimeException(
			"If S1Reservoir Histogram is set, D2Combiner histogram must also be set in the config file!");
	    }
	    HistogramType srcHistType = HistogramType.D2_COMB_HIST;
	    currentBolt = MyUtilities.attachEmitterFilteredRangeMulticast(
		    SystemParameters.D2_TO_S1_STREAM, conf, comparison,
		    wrapper, dstHistType, srcHistType, d2Reduce.getName(),
		    currentBolt, d2Reduce);

	    // acks from d2Reduce
	    currentBolt = MyUtilities.attachEmitterHash(conf, null,
		    currentBolt, d2Reduce);

	    // all the data from s1Source
	    currentBolt = MyUtilities.attachEmitterRange(_conf, _wrapper,
		    dstHistType, currentBolt, s1Source);
	}

	if (MyUtilities.isAutoOutputSampleSize(conf)) {
	    // from Partitioner we receive the size of output sample
	    currentBolt = MyUtilities.attachEmitterBroadcast(
		    SystemParameters.FROM_PARTITIONER, currentBolt,
		    partitionerName);
	}

	// manually setting reservoir size
	if (!MyUtilities.isAutoOutputSampleSize(conf)) {
	    int outputSampleSize = MyUtilities.computeManualSampleSize(conf,
		    _firstNumOfBuckets, _secondNumOfBuckets);
	    LOG.info("Manually setting outputSampleSize to " + outputSampleSize);
	    constructReservoir(outputSampleSize);
	}

	if (_hierarchyPosition == StormComponent.FINAL_COMPONENT) {
	    killer.registerComponent(this, componentName, parallelism);
	}
    }

    private void constructReservoir(int outputSampleSize) {
	_reservoir = new FixedSizePriorityQueue<KeyPriorityProbability>(
		outputSampleSize,
		new KeyPriorityProbability.KeyPriorityComparator());
    }

    private void processNonLastTuple(String inputComponentIndex,
	    String sourceStreamId, List<String> tuple) {
	if (inputComponentIndex.equals(_d2CombinerIndex)) {
	    String strKey = tuple.get(0);
	    JAT key = (JAT) _wrapper.fromString(strKey);
	    String strMult = tuple.get(1);
	    int mult = Integer.parseInt(strMult);
	    if (!_isEWHS1Histogram) {
		maxMultiplicity(_d2, key, mult);
	    } else {
		// d2_equi arrives, without duplicates (across boundaries)
		if (_isSinglePassD2Equi) {
		    // for now update only _d2Equi, and later build _d2 in a
		    // single pass using createD2OutOfD2Equi
		    // addMultiplicity(_d2Equi, key, mult);
		    JAT beginKey = (JAT) _wrapper.getOffset(key, -_band);
		    addMultiplicity(_d2BeginEnd, beginKey, mult);
		    JAT endKey = (JAT) _wrapper.getOffset(key, _band + 1);
		    addMultiplicity(_d2BeginEnd, endKey, -mult);
		    if (_isSinglePassDebug) {
			addMultiplicityJoin(_d2OldDebug, key, mult);
		    }
		} else {
		    addMultiplicityJoin(_d2, key, mult);
		}
	    }
	} else if (inputComponentIndex.equals(_s1SourceIndex)) {
	    String strKey = tuple.get(0);
	    addMultiplicity(_r1, strKey, 1);
	} else if (inputComponentIndex.equals(_partitionerIndex)) {
	    String strSampleSize = tuple.get(1);
	    int outputSampleSize = Integer.parseInt(strSampleSize);
	    constructReservoir(outputSampleSize);

	    // sending output sample size information to S1Merge
	    List<Integer> hashIndexes = new ArrayList<Integer>(Arrays.asList(0)); // does
										  // not
										  // matter
	    tupleSend(SystemParameters.RESERVOIR_TO_MERGE, tuple, hashIndexes);
	} else {
	    throw new RuntimeException("In S1ReduceBolt, unrecognized source "
		    + inputComponentIndex);
	}
    }

    private <T> void addMultiplicity(Map<T, Integer> stats, T key, int mult) {
	if (stats.containsKey(key)) {
	    mult += stats.get(key);
	}
	stats.put(key, mult);
    }

    private void addMultiplicityJoin(Map<JAT, Integer> stats, JAT key, int mult) {
	List<JAT> joinableKeys = _comparison.getJoinableKeys(key);
	for (JAT joinableKey : joinableKeys) {
	    // String joinableStrKey = MyUtilities.toSpecialString(joinableKey,
	    // _wrapper); not needed anymore
	    addMultiplicity(stats, joinableKey, mult);
	}
    }

    // at D2Combiner, one machine which is assigned that very key will get all
    // the joinable set from R2 (R2->D2Combiner uses RangeMulticast)
    private void maxMultiplicity(Map<JAT, Integer> stats, JAT key, int mult) {
	if (stats.containsKey(key)) {
	    if (mult > stats.get(key)) {
		stats.put(key, mult);
	    }
	} else {
	    stats.put(key, mult);
	}
    }

    private void finalizeProcessing() {
	LOG.info("Received data from from all D2Combiner/R1 bolts. About to start creating reservoir...");
	if (_isEWHS1Histogram && _isSinglePassD2Equi) {
	    // we compute _d2 out of _d2Equi
	    createD2OutOfD2Equi();
	}

	if (SystemParameters.getBooleanIfExist(_conf, "D2_DEBUG_MODE")) {
	    // this is just for local debugging
	    LOG.info("d2 in S1ReservoirGenerator is " + _d2);
	}

	// creating reservoir
	if (!_d2.isEmpty()) {
	    // no need to iterate over _r1 if _d2 is empty: _d2 could be empty
	    // as equi-depth on R1 and on R2 can vastly differ (see Bicd)
	    int r2RelationSize = getR2RelationSize();
	    for (Map.Entry<String, Integer> entry : _r1.entrySet()) {
		String r1StrKey = entry.getKey();
		JAT r1Key = (JAT) _wrapper.fromString(r1StrKey);
		int r1Mult = entry.getValue();
		int d2Mult;
		if ((d2Mult = getMultiplicity(_d2, r1Key)) > 0) {
		    // there is no joinable tuple from d2, so the weight and
		    // priority is 0
		    double d2KeyProbability = ((double) d2Mult)
			    / r2RelationSize;
		    double power = 1.0 / d2Mult;
		    for (int i = 0; i < r1Mult; i++) {
			// there may be multiple rows from R1 with the same key
			// for each of them a separate random generator
			// invocation
			double priority = Math.pow(_rndGen.nextDouble(), power);
			KeyPriorityProbability keyPriority = new KeyPriorityProbability(
				r1StrKey, priority, d2KeyProbability);
			_reservoir.add(keyPriority); // add method is changed in
						     // the implementation such
						     // that we do not violate
						     // capacity constraints
		    }

		    // totalOutputs update
		    _computedTotalOutputs += ((long) r1Mult) * d2Mult;
		}
	    }
	}

	// sending reservoir
	KeyPriorityProbability kp;
	LOG.info("I am sending reservoir of size " + _reservoir.size()
		+ " (should be (at most) outputSampleSize large).");
	while ((kp = _reservoir.poll()) != null) {
	    String strKey = kp.getKey();
	    double priority = kp.getPriority();
	    String strPriority = String.valueOf(priority);
	    double d2KeyProbability = kp.getD2KeyProbability();
	    String strD2KeyProbability = String.valueOf(d2KeyProbability);

	    List<String> tuple = new ArrayList<String>(Arrays.asList(strKey,
		    strPriority, strD2KeyProbability));
	    List<Integer> hashIndexes = new ArrayList<Integer>(Arrays.asList(0));
	    tupleSend(SystemParameters.RESERVOIR_TO_MERGE, tuple, hashIndexes);
	    if (SystemParameters.getBooleanIfExist(_conf, "DEBUG_MODE")) {
		// this is just for local debugging
		LOG.info("Reservoir element in S1ReservoirGenerator is "
			+ tuple);
	    }
	}

	// sending total number of output tuples
	Values totalOutpuSize = MyUtilities.createTotalOutputSizeTuple(
		_componentIndex, _computedTotalOutputs);
	_collector.emit(SystemParameters.PARTITIONER, totalOutpuSize);

	LOG.info("Reservoir creation and sending is completed.");
    }

    // assumes TreeMap where non-mentioned elements have value of their closest
    // left neighbor
    private int getMultiplicity(TreeMap<JAT, Integer> d2, JAT r1Key) {
	Entry<JAT, Integer> repositionedEntry = d2.floorEntry(r1Key);
	if (repositionedEntry == null) {
	    return 0;
	} else {
	    return repositionedEntry.getValue();
	}
    }

    /*
     * //If Opt2 is not used private int getMultiplicity(TreeMap<JAT, Integer>
     * d2, JAT r1Key) { if(d2.containsKey(r1Key){ return d2.get(r1Key); }else{
     * return 0; } }
     */

    // Uses _d2Begin and _d2End (total number of accesses is 2 * #(d2Equi))
    private void createD2OutOfD2Equi() {
	if (!_d2BeginEnd.isEmpty()) {
	    int multInRange = 0;
	    // basically we are doing prefix sum
	    for (Map.Entry<JAT, Integer> entry : _d2BeginEnd.entrySet()) {
		multInRange += entry.getValue(); // can be negative when exiting
						 // out of scope of a key
		entry.setValue(multInRange); // updating in place: we won't need
					     // previous values anymore
	    }
	    _d2 = _d2BeginEnd;

	    if (_isSinglePassDebug) {
		LOG.info("Don't use it in production: checking whether single pass computes d2 correctly...");
		// checking if the result is as expected
		StringBuilder sb = new StringBuilder();
		if (!MyUtilities.isMapsEqual(_d2, _d2OldDebug, sb)) {
		    throw new RuntimeException(
			    "_d2 and _d2OldDebug are not equal! "
				    + sb.toString());
		}
	    }
	}
    }

    /*
     * Old version uses _d2Equi private void createD2OutOfD2Equi(){
     * if(!_d2Equi.isEmpty()){ // _d2Equi could be empty as equi-depth on R1 and
     * on R2 can vastly differ (see Bicd) JAT firstKey = _d2Equi.firstKey(); JAT
     * lastKey = _d2Equi.lastKey(); int multInRange = 0; for(JAT currentKey =
     * (JAT) _wrapper.getOffset(firstKey, -_band); currentKey.compareTo((JAT)
     * _wrapper.getOffset(lastKey, _band + 1)) < 0; currentKey =
     * (JAT)_wrapper.minIncrement(currentKey)){
     * 
     * // entering into range // check if there is a d2Equi 'band' positions to
     * the right JAT enteringKey = (JAT) _wrapper.getOffset(currentKey, _band);
     * if (_d2Equi.containsKey(enteringKey)){ multInRange +=
     * _d2Equi.get(enteringKey); } //exiting from range // check if there is a
     * d2Equi 'band + 1' positions to the left JAT exitingKey = (JAT)
     * _wrapper.getOffset(currentKey, -(_band + 1));
     * if(_d2Equi.containsKey(exitingKey)){ multInRange -=
     * _d2Equi.get(exitingKey); }
     * 
     * if(multInRange > 0){ //writing this into data structure
     * _d2.put(currentKey, multInRange); }else{ //let's move to the first next
     * non-zero position // +1 because we will increment at the end of this
     * iteration // we can use method higherKey because all the keys are unique
     * in _d2Equi JAT nextPositivePos = (JAT)
     * _wrapper.getOffset(_d2Equi.higherKey(currentKey), -(_band + 1));
     * if(currentKey.compareTo(nextPositivePos) < 0){ currentKey =
     * nextPositivePos; } } }
     * 
     * if(_isSinglePassDebug){ LOG.info(
     * "Don't use it in production: checking whether single pass computes d2 correctly..."
     * ); // checking if the result is as expected StringBuilder sb = new
     * StringBuilder(); if(!MyUtilities.isMapsEqual(_d2, _d2OldDebug, sb)){
     * throw new RuntimeException("_d2 and _d2OldDebug are not equal! " +
     * sb.toString()); } } } }
     */

    private int getR2RelationSize() {
	if (SystemParameters.getBoolean(_conf, "IS_FIRST_D2")) {
	    return SystemParameters.getInt(_conf, "FIRST_REL_SIZE");
	} else {
	    return SystemParameters.getInt(_conf, "SECOND_REL_SIZE");
	}
    }

    // BaseRichSpout
    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
	_collector = collector;
	_numRemainingParents = MyUtilities.getNumParentTasks(tc,
		Arrays.asList(_d2Combiner, _s1Source));
	if (MyUtilities.isAutoOutputSampleSize(_conf)) {
	    _numRemainingParents += 1; // partitioner parallelism is always 1
	}
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

	    final List<String> outputFieldsMerge = new ArrayList<String>();
	    outputFieldsMerge.add(StormComponent.COMP_INDEX);
	    outputFieldsMerge.add(StormComponent.TUPLE); // list of string
	    outputFieldsMerge.add(StormComponent.HASH);
	    declarer.declareStream(SystemParameters.RESERVOIR_TO_MERGE,
		    new Fields(outputFieldsMerge));

	    final List<String> outputFieldsPart = new ArrayList<String>();
	    outputFieldsPart.add(StormComponent.COMP_INDEX);
	    outputFieldsPart.add(StormComponent.TUPLE); // list of string
	    outputFieldsPart.add(StormComponent.HASH);
	    declarer.declareStream(SystemParameters.PARTITIONER, new Fields(
		    outputFieldsPart));
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
	    MyUtilities.processFinalAckCustomStream(
		    SystemParameters.PARTITIONER, _numRemainingParents,
		    StormComponent.INTERMEDIATE, _conf, stormTupleRcv,
		    _collector);
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