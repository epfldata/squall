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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.ewh.algorithms.BSPAlgorithm;
import ch.epfl.data.squall.ewh.algorithms.InputOutputShallowCoarsener;
import ch.epfl.data.squall.ewh.algorithms.ShallowCoarsener;
import ch.epfl.data.squall.ewh.algorithms.TilingAlgorithm;
import ch.epfl.data.squall.ewh.algorithms.optimality.MaxAvgOptimality;
import ch.epfl.data.squall.ewh.algorithms.optimality.OptimalityMetricInterface;
import ch.epfl.data.squall.ewh.algorithms.optimality.WeightFunction;
import ch.epfl.data.squall.ewh.data_structures.JoinMatrix;
import ch.epfl.data.squall.ewh.data_structures.KeyRegion;
import ch.epfl.data.squall.ewh.data_structures.Region;
import ch.epfl.data.squall.ewh.data_structures.UJMPAdapterIntMatrix;
import ch.epfl.data.squall.ewh.main.PushStatisticCollector;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.StormEmitter;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.utilities.DeepCopy;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;

public class EWHSampleMatrixBolt<JAT extends Number & Comparable<JAT>> extends
	BaseRichBolt {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(EWHSampleMatrixBolt.class);

    private StormEmitter _firstEmitter, _secondEmitter, _outputSampler,
	    _s1ReservoirGenerator;
    private final String _firstEmitterIndex, _secondEmitterIndex,
	    _outputSamplerIndex, _s1ReservoirGeneratorIndex, _componentIndex;
    private final String _componentName;
    private NumericType _wrapper;
    private ComparisonPredicate _comparison;
    private Map _conf;
    private OutputCollector _collector;

    private int _xRelationSize, _yRelationSize;
    private int _xNumOfBuckets, _yNumOfBuckets;
    private int _numRemainingParents;
    private int _numOfLastJoiners; // input to tiling algorithm

    // used in second run
    private int _xComputedRelSize, _yComputedRelSize;
    private long _computedTotalOutputSize;
    private int _computedTotalOutputSizeCounter = 0;
    private int _s1ReservoirGeneratorPar = 0;
    private int _outputSampleSize; // computed based on the join matrix and
				   // candidates

    // received from three parents
    private List<JAT> _xJoinKeys = new ArrayList<JAT>();
    private List<JAT> _yJoinKeys = new ArrayList<JAT>();
    private List<TwoString> _outputSample = new ArrayList<TwoString>();

    private Map<JAT, FirstPosFreq> _xFirstPosFreq = new HashMap<JAT, FirstPosFreq>();
    private Map<JAT, FirstPosFreq> _yFirstPosFreq = new HashMap<JAT, FirstPosFreq>();

    // used only when SAMPLE_MATRIX_EDGE_PROB = true
    private List<Integer> _xBoundaryPositions, _yBoundaryPositions;
    // maps boundary keys to a list of (begin probabilities, bucket position)
    // it suffices for a boundary key to do a binary search over begin
    // probabilities (using a random number)
    private Map<JAT, ArrayList<BeginProbBucket>> _xProbForBoundaryKeys,
	    _yProbForBoundaryKeys;

    // each JAT is a beginning of the bucket;
    // except first-beg, which is considered -inf, and
    // last-end, which is considered +inf.
    private List<JAT> _xBoundaries, _yBoundaries;

    private Random _rndGen = new Random();

    private JoinMatrix<JAT> _joinMatrix;

    // state
    private enum STATE {
	CREATE_INPUT_SAMPLE, CREATE_MATRIX
    };

    private STATE _state = STATE.CREATE_INPUT_SAMPLE;

    public EWHSampleMatrixBolt(StormEmitter firstEmitter,
	    StormEmitter secondEmitter, StormEmitter outputSampler,
	    StormEmitter s1ReservoirGenerator, String componentName,
	    int numOfLastJoiners, int firstRelationSize,
	    int secondRelationSize, NumericType<JAT> wrapper,
	    ComparisonPredicate comparison, int firstNumOfBuckets,
	    int secondNumOfBuckets, List<String> allCompNames,
	    TopologyBuilder builder, TopologyKiller killer, Config conf) {

	_firstEmitter = firstEmitter;
	_secondEmitter = secondEmitter;
	_outputSampler = outputSampler;
	_s1ReservoirGenerator = s1ReservoirGenerator;
	_firstEmitterIndex = String.valueOf(allCompNames.indexOf(firstEmitter
		.getName()));
	_secondEmitterIndex = String.valueOf(allCompNames.indexOf(secondEmitter
		.getName()));
	_outputSamplerIndex = String.valueOf(allCompNames.indexOf(outputSampler
		.getName()));
	_s1ReservoirGeneratorIndex = String.valueOf(allCompNames
		.indexOf(s1ReservoirGenerator.getName()));

	_s1ReservoirGeneratorPar = SystemParameters.getInt(conf,
		s1ReservoirGenerator.getName() + "_PAR");

	_componentName = componentName;
	_componentIndex = String.valueOf(allCompNames.indexOf(componentName));

	_numOfLastJoiners = numOfLastJoiners;
	_conf = conf;
	_comparison = comparison;
	_wrapper = wrapper;

	_xRelationSize = firstRelationSize;
	_yRelationSize = secondRelationSize;
	_xNumOfBuckets = firstNumOfBuckets;
	_yNumOfBuckets = secondNumOfBuckets;

	final int parallelism = 1;

	// connecting with previous level
	InputDeclarer currentBolt = builder.setBolt(componentName, this,
		parallelism);

	currentBolt = MyUtilities.attachEmitterToSingle(
		SystemParameters.PARTITIONER, currentBolt, firstEmitter,
		secondEmitter, outputSampler, s1ReservoirGenerator);

	// connecting with Killer
	// if (getHierarchyPosition() == FINAL_COMPONENT &&
	// (!MyUtilities.isAckEveryTuple(conf)))
	killer.registerComponent(this, componentName, parallelism);
    }

    @Override
    public void execute(Tuple stormTupleRcv) {
	final String inputComponentIndex = stormTupleRcv
		.getStringByField(StormComponent.COMP_INDEX); // getString(0);
	final List<String> tuple = (List<String>) stormTupleRcv
		.getValueByField(StormComponent.TUPLE); // getValue(1);

	if (processFinalAck(tuple, stormTupleRcv, inputComponentIndex))
	    return;

	processNonLastTuple(inputComponentIndex, tuple, stormTupleRcv, true);

	_collector.ack(stormTupleRcv);
    }

    // from IRichBolt
    @Override
    public Map<String, Object> getComponentConfiguration() {
	return _conf;
    }

    private void processNonLastTuple(String inputComponentIndex,
	    List<String> tuple, Tuple stormTupleRcv, boolean isLastInBatch) {
	if (_firstEmitterIndex.equals(inputComponentIndex)) {
	    if (MyUtilities.isRelSize(tuple)) {
		_xComputedRelSize += Integer.parseInt(tuple.get(1));
	    } else {
		// R update
		String key = tuple.get(0); // key is the only thing sent
		_xJoinKeys.add((JAT) _wrapper.fromString(key));
	    }
	} else if (_secondEmitterIndex.equals(inputComponentIndex)) {
	    if (MyUtilities.isRelSize(tuple)) {
		_yComputedRelSize += Integer.parseInt(tuple.get(1));
	    } else {
		// S update
		String key = tuple.get(0); // key is the only thing sent
		_yJoinKeys.add((JAT) _wrapper.fromString(key));
	    }
	} else if (_outputSamplerIndex.equals(inputComponentIndex)) {
	    _outputSample.add(new TwoString(tuple));
	} else if (_s1ReservoirGeneratorIndex.equals(inputComponentIndex)) {
	    _computedTotalOutputSize += Long.parseLong(tuple.get(1));
	    _computedTotalOutputSizeCounter++;
	} else
	    throw new RuntimeException("InputComponentName "
		    + inputComponentIndex + " doesn't match neither "
		    + _firstEmitterIndex + " nor " + _secondEmitterIndex + ".");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	// if (_hierarchyPosition == FINAL_COMPONENT) { // then its an
	// intermediate
	// stage not the final
	// one
	// if (!MyUtilities.isAckEveryTuple(_conf))
	declarer.declareStream(SystemParameters.EOF_STREAM, new Fields(
		SystemParameters.EOF));

	// because of sending n_c_s and totalOutputSampleSize
	if (MyUtilities.isAutoOutputSampleSize(_conf)) {
	    final List<String> outputFieldsDef = new ArrayList<String>();
	    outputFieldsDef.add(StormComponent.COMP_INDEX);
	    outputFieldsDef.add(StormComponent.TUPLE); // list of string
	    outputFieldsDef.add(StormComponent.HASH);
	    declarer.declareStream(SystemParameters.FROM_PARTITIONER,
		    new Fields(outputFieldsDef));
	}
    }

    // BaseRichSpout
    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
	_collector = collector;

	// when all of these are done, we will wait for _outputSampler,
	// _s1ReservoirGenerator
	_numRemainingParents = MyUtilities.getNumParentTasks(tc,
		Arrays.asList(_firstEmitter, _secondEmitter));
	// _numRemainingParents = MyUtilities.getNumParentTasks(tc,
	// Arrays.asList(_firstEmitter, _secondEmitter, _outputSampler,
	// _s1ReservoirGenerator));
    }

    private void tupleSend(String streamId, List<String> tuple,
	    List<Integer> hashIndexes) {
	final Values stormTupleSnd = MyUtilities.createTupleValues(tuple, 0,
		_componentIndex, hashIndexes, null, _conf);
	MyUtilities.sendTuple(streamId, stormTupleSnd, null, _collector, _conf);
    }

    // if true, we should exit from method which called this method
    protected boolean processFinalAck(List<String> tuple, Tuple stormTupleRcv,
	    String inputComponentIndex) {
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

	    if (_state == STATE.CREATE_INPUT_SAMPLE) {
		if (MyUtilities.isAutoOutputSampleSize(_conf)) {
		    MyUtilities.processFinalAckCustomStream(
			    SystemParameters.FROM_PARTITIONER,
			    _numRemainingParents, StormComponent.INTERMEDIATE,
			    _conf, stormTupleRcv, _collector);
		}

		// out design assumes that in this state we get ACKS only from
		// first and secondParent
		// if we get ACK from other emitters, we need to count them
		// separately
		String source = stormTupleRcv.getSourceComponent();
		if (!(source.equals(_firstEmitter.getName()) || source
			.equals(_secondEmitter.getName()))) {
		    throw new RuntimeException(
			    "Concurrency bug is to be fixed! " + source);
		}
	    } else if (_state == STATE.CREATE_MATRIX) {
		MyUtilities.processFinalAck(_numRemainingParents,
			StormComponent.FINAL_COMPONENT, _conf, stormTupleRcv,
			_collector);
	    } else {
		throw new RuntimeException("Should not be here!");
	    }

	    // advancing the state
	    if (_state == STATE.CREATE_INPUT_SAMPLE
		    && _numRemainingParents == 0) {
		_state = STATE.CREATE_MATRIX;
		_numRemainingParents = SystemParameters.getInt(_conf,
			_outputSampler.getName() + "_PAR")
			+ SystemParameters.getInt(_conf,
				_s1ReservoirGenerator.getName() + "_PAR");
	    }

	    return true;
	}
	return false;
    }

    private void finalizeProcessing() {
	if (_state == STATE.CREATE_INPUT_SAMPLE) {
	    long start = System.currentTimeMillis();
	    createJoinMatrix();
	    double elapsed = (System.currentTimeMillis() - start) / 1000.0;
	    LOG.info("Creating join matrix takes " + elapsed + " seconds.");
	    if (MyUtilities.isAutoOutputSampleSize(_conf)) {
		start = System.currentTimeMillis();
		computeNumCandidates();
		sendNumCandidates();
		elapsed = (System.currentTimeMillis() - start) / 1000.0;
		LOG.info("Computing num of candidates takes " + elapsed
			+ " seconds.");
	    }
	} else if (_state == STATE.CREATE_MATRIX) {
	    // put scaled output sample into join matrix
	    long start = System.currentTimeMillis();
	    fillOutput(_joinMatrix);
	    double elapsed = (System.currentTimeMillis() - start) / 1000.0;
	    LOG.info("Filling join matrix takes " + elapsed + " seconds.");

	    // run algorithms
	    start = System.currentTimeMillis();
	    createKeyRegions();
	    elapsed = (System.currentTimeMillis() - start) / 1000.0;
	    LOG.info("Creating key regions including algorithm(s) invocation takes "
		    + elapsed + " seconds.");
	} else {
	    throw new RuntimeException("Should not be here!");
	}
    }

    private void createJoinMatrix() {
	// sort keys
	LOG.info("After creation of xJoinKeys and yJoinKeys (2 * n_s * SystemParameters.TUPLES_PER_BUCKET), memory usage is "
		+ MyUtilities.getUsedMemoryMBs() + " MBs.");
	LOG.info("Before sorting keys");
	Collections.sort(_xJoinKeys);
	Collections.sort(_yJoinKeys);
	LOG.info("After sorting keys");
	LOG.info("FirstKeys size is " + _xJoinKeys.size());
	LOG.info("SecondKeys size is " + _yJoinKeys.size());

	if (SystemParameters
		.getBooleanIfExist(_conf, "SAMPLE_MATRIX_EDGE_PROB")) {
	    _xBoundaryPositions = new ArrayList<Integer>();
	    _yBoundaryPositions = new ArrayList<Integer>();
	}
	// create bucket boundaries
	// choose keys equi-distantly such that in total there are _numOfBuckets
	// of them
	// When SAMPLE_MATRIX_EDGE_PROB = true, also set boundaryPositions
	_xBoundaries = createBoundaries(_xJoinKeys, _xNumOfBuckets,
		_xBoundaryPositions);
	_yBoundaries = createBoundaries(_yJoinKeys, _yNumOfBuckets,
		_yBoundaryPositions);
	// xSize = min (xNumOfBuckets, xRelationSize)
	int xSize = _xBoundaries.size();
	int ySize = _yBoundaries.size();

	LOG.info("FirstBoundaries size is " + xSize);
	LOG.info("SecondBoundaries size is " + ySize);

	if (!SystemParameters.getBoolean(_conf, "DIP_DISTRIBUTED")) {
	    // this is just for local debugging
	    LOG.info("FirstBoundaries are " + _xBoundaries);
	    LOG.info("SecondBoundaries are " + _yBoundaries);
	}

	// create matrix and fill it with the joinAttributes (keys)
	_joinMatrix = new UJMPAdapterIntMatrix(xSize, ySize, _conf,
		_comparison, _wrapper);
	LOG.info("Capacity of joinMatrix in BSP is "
		+ _joinMatrix.getCapacity());
	for (int i = 0; i < xSize; i++) {
	    _joinMatrix.setJoinAttributeX(_xBoundaries.get(i));
	}
	for (int i = 0; i < ySize; i++) {
	    _joinMatrix.setJoinAttributeY(_yBoundaries.get(i));
	}
    }

    private void computeNumCandidates() {
	int n_c_s = _joinMatrix.getNumCandidatesIterate(_conf);
	LOG.info("The number of candidate cells in the sample matrix is "
		+ n_c_s);
	int multiplier = SystemParameters.getInt(_conf,
		"OUTPUT_SAMPLE_SIZE_CONSTANT");
	_outputSampleSize = multiplier * n_c_s;

	// check capacity violation
	long capacity = _joinMatrix.getCapacity();
	if (capacity == -1) {
	    LOG.info("Cannot check capacity, as the matrix is created by reading from a file.");
	} else {
	    if (capacity < n_c_s) {
		throw new RuntimeException(
			"Please increase joinMatrix capacity from " + capacity
				+ " to at least n_c_s = " + n_c_s);
	    }
	}
    }

    private void sendNumCandidates() {
	// sending to s1ReservoirGenerator
	String strOutputSampleSize = String.valueOf(_outputSampleSize);
	List<String> tupleSampleSize = new ArrayList<String>(Arrays.asList(
		SystemParameters.OUTPUT_SAMPLE_SIZE, strOutputSampleSize));
	List<Integer> hashIndexes = new ArrayList<Integer>(Arrays.asList(0)); // does
									      // not
									      // matter
	tupleSend(SystemParameters.FROM_PARTITIONER, tupleSampleSize,
		hashIndexes);
    }

    private List<JAT> createBoundaries(List<JAT> joinKeys, int numOfBuckets,
	    List<Integer> boundaryPosition) {
	if (numOfBuckets > joinKeys.size()) {
	    numOfBuckets = joinKeys.size();
	}
	double distance = ((double) joinKeys.size()) / numOfBuckets;
	LOG.info("Distance = " + distance);
	if (distance < 1) {
	    throw new RuntimeException(
		    "A bug: same element cannot be included more than once!");
	}

	List<JAT> boundaries = new ArrayList<JAT>();
	for (int i = 0; i < numOfBuckets; i++) {
	    // Old comments: distance is around
	    // SystemParameters.TUPLES_PER_BUCKET
	    // rounding error exists only for the very last 2D bucket: max error
	    // is 2X * 2X = 4X
	    // if we use (int) (index + 0.5), the error is 1/2 * 1/2 = 0.25x (4
	    // times reduction)
	    // We want to avoid high discrepancy between bucket sizes, which is
	    // a consequence of input sample size != 100 * n_s
	    int index = (int) (i * distance + 0.5);
	    if (boundaryPosition != null) {
		boundaryPosition.add(index);
	    }
	    boundaries.add(joinKeys.get(index));
	}

	// beginning of the bucket 0 is -infinity
	boundaries.set(0, (JAT) _wrapper.getMinValue());

	return boundaries;
    }

    /*
     * old version private List<JAT> createBoundaries(List<JAT> joinKeys, int
     * numOfBuckets) { if(numOfBuckets > joinKeys.size()){ numOfBuckets =
     * joinKeys.size(); } int distance = joinKeys.size() / numOfBuckets;
     * LOG.info("Distance = " + distance); if(distance < 1){ throw new
     * RuntimeException
     * ("A bug: same element cannot be included more than once!"); }
     * 
     * List<JAT> boundaries = new ArrayList<JAT>(); for(int i = 0; i <
     * numOfBuckets; i++){ // distance is around
     * SystemParameters.TUPLES_PER_BUCKET // rounding error exists only for the
     * very last 2D bucket: max error is 2X * 2X = 4X // if we use (int) (index
     * + 0.5), the error is 1/2 * 1/2 = 0.25x (4 times reduction) int index = i
     * * distance; boundaries.add(joinKeys.get(index)); }
     * 
     * //beginning of the bucket 0 is -infinity boundaries.set(0,
     * (JAT)_wrapper.getMinValue());
     * 
     * return boundaries; }
     */

    // filling sample matrix with output and computing key regions
    private void createKeyRegions() {
	// printouts
	if (MyUtilities.isAutoOutputSampleSize(_conf)) {
	    LOG.info("Automatic setting outputSampleSize to "
		    + _outputSampleSize);
	}
	LOG.info("Computed xRelSize is " + _xComputedRelSize);
	LOG.info("Computed yRelSize is " + _yComputedRelSize);
	LOG.info("Computed total output size is " + _computedTotalOutputSize);

	if (_computedTotalOutputSizeCounter != _s1ReservoirGeneratorPar) {
	    throw new RuntimeException(
		    "Fatal error! Did not compute totalOutputSize correctly. Please fall back to manually specifying output.");
	}

	// create algos
	StringBuilder sb = new StringBuilder();
	double weightInput = SystemParameters.getDouble(_conf, "WF_INPUT");
	double weightOutput = SystemParameters.getDouble(_conf, "WF_OUTPUT");
	LOG.info("Weight Input = " + weightInput + ", Weight Output = "
		+ weightOutput);
	WeightFunction wf = new WeightFunction(weightInput, weightOutput);
	// this is for rounded matrix
	int bspP = 50;
	if (SystemParameters.isExisting(_conf, "DIP_BSP_P")) {
	    bspP = SystemParameters.getInt(_conf, "DIP_BSP_P");
	}
	// not sure if still a problem: !!!! WATCH OUT: IF YOU USE TWO BSP
	// ALGORITHMS, CHECK WHAT DO YOU DO WITH ORIGINAL JOIN_MATRIX
	// should have used joinMatrix.getDeepCopy, previously it did not work
	List<TilingAlgorithm> algorithms = new ArrayList<TilingAlgorithm>();
	// io
	ShallowCoarsener inputOutputCoarsener = new InputOutputShallowCoarsener(
		bspP, bspP, wf, _conf);
	algorithms.add(new BSPAlgorithm(_conf, _numOfLastJoiners, wf,
		inputOutputCoarsener, BSPAlgorithm.COVERAGE_MODE.SPARSE));
	// rest
	// ShallowCoarsener inputCoarsener = new InputShallowCoarsener(bspP,
	// bspP);
	// algorithms.add(new BSPAlgorithm(_conf, _numOfLastJoiners, wf,
	// inputCoarsener, BSPAlgorithm.COVERAGE_MODE.SPARSE));
	// ShallowCoarsener outputCoarsener = new OutputShallowCoarsener(bspP,
	// bspP, wf, _conf);
	// algorithms.add(new BSPAlgorithm(_conf, _numOfLastJoiners, wf,
	// outputCoarsener, BSPAlgorithm.COVERAGE_MODE.SPARSE));

	// run algos
	for (TilingAlgorithm algorithm : algorithms) {
	    try {
		long startTime = System.currentTimeMillis();
		sb = new StringBuilder();
		List<Region> regions = algorithm.partition(_joinMatrix, sb);
		long endTime = System.currentTimeMillis();
		double elapsed = (endTime - startTime) / 1000.0;
		// sbs are never printed out

		// compute the joiner regions
		List<KeyRegion> keyRegions = PushStatisticCollector
			.generateKeyRegions(regions, _joinMatrix, _wrapper);
		// we serialize and deserialize according to what is demanded,
		// not what was the actual number of buckets
		String keyRegionFilename = MyUtilities.getKeyRegionFilename(
			_conf, algorithm.getShortName(), _numOfLastJoiners,
			bspP);
		LOG.info("keyRegionFilename = " + keyRegionFilename);

		// write KeyRegions
		// "Most impressive is that the entire process is JVM
		// independent,
		// meaning an object can be serialized on one platform and
		// deserialized on an entirely different platform."
		DeepCopy.serializeToFile(keyRegions, keyRegionFilename);
		keyRegions = (List<KeyRegion>) DeepCopy
			.deserializeFromFile(keyRegionFilename);
		LOG.info("Algorithm " + algorithm.toString() + " has "
			+ KeyRegion.toString(keyRegions));

		// compute the optimality
		OptimalityMetricInterface opt = new MaxAvgOptimality(
			_joinMatrix, regions, algorithm.getPrecomputation());
		// print regions
		LOG.info("Final regions are: "
			+ Region.toString(regions, opt, "Final"));
		LOG.info("\nElapsed algorithm time (including all the subparts) is "
			+ elapsed + " seconds.\n");
		LOG.info("\n=========================================================================================\n");

		/*
		 * // read the existing KeyRegions (probably by BSP) String
		 * filenameBsp = MyUtilities.getKeyRegionFilename(_conf);
		 * keyRegions = (List<KeyRegion>)
		 * DeepCopy.deserializeFromFile(filenameBsp);
		 * LOG.info("PREVIOUS FILE " + KeyRegion.toString(keyRegions));
		 */
	    } catch (Exception exc) {
		LOG.info("EXCEPTION" + MyUtilities.getStackTrace(exc));
	    }
	}
    }

    private void fillOutput(JoinMatrix<JAT> joinMatrix) {
	long start = System.currentTimeMillis();
	if (!SystemParameters.getBooleanIfExist(_conf,
		"SAMPLE_MATRIX_EDGE_PROB")) {
	    // default way: imprecise
	    precomputeFirstPosFreq(_xBoundaries, _xFirstPosFreq);
	    precomputeFirstPosFreq(_yBoundaries, _yFirstPosFreq);
	} else {
	    _xProbForBoundaryKeys = new HashMap<JAT, ArrayList<BeginProbBucket>>();
	    _yProbForBoundaryKeys = new HashMap<JAT, ArrayList<BeginProbBucket>>();
	    precomputeProbForBoundaryKeys(_xJoinKeys, _xBoundaries,
		    _xBoundaryPositions, _xProbForBoundaryKeys);
	    LOG.info("Precomputation of X finished.");
	    precomputeProbForBoundaryKeys(_yJoinKeys, _yBoundaries,
		    _yBoundaryPositions, _yProbForBoundaryKeys);
	    LOG.info("Precomputation of Y finished.");
	}
	double elapsed = (System.currentTimeMillis() - start) / 1000.0;
	LOG.info("Part of Filling join matrix: Precomputation takes " + elapsed
		+ " seconds.");
	start = System.currentTimeMillis();
	for (TwoString outputSampleTuple : _outputSample) {
	    String xStrKey = outputSampleTuple.getX();
	    String yStrKey = outputSampleTuple.getY();
	    JAT xKey = (JAT) _wrapper.fromString(xStrKey);
	    JAT yKey = (JAT) _wrapper.fromString(yStrKey);
	    int x = -1;
	    int y = -1;
	    if (!SystemParameters.getBooleanIfExist(_conf,
		    "SAMPLE_MATRIX_EDGE_PROB")) {
		// default way: imprecise
		x = findBucket(_xBoundaries, _xFirstPosFreq, xKey);
		y = findBucket(_yBoundaries, _yFirstPosFreq, yKey);
	    } else {
		x = findBucket(xKey, _xBoundaries, _xProbForBoundaryKeys);
		y = findBucket(yKey, _yBoundaries, _yProbForBoundaryKeys);
	    }
	    joinMatrix.increment(x, y);
	}
	elapsed = (System.currentTimeMillis() - start) / 1000.0;
	LOG.info("Part of Filling join matrix: Loop of adding output takes "
		+ elapsed + " seconds.");
	start = System.currentTimeMillis();
	// we first place the elements inside, and then scale them
	// by doing so, we reduce double-to-int rounding errors
	scaleOutput(joinMatrix);
	elapsed = (System.currentTimeMillis() - start) / 1000.0;
	LOG.info("Part of Filling join matrix: Scale output takes " + elapsed
		+ " seconds.");
    }

    // only SAMPLE_MATRIX_EDGE_PROB = false
    private void precomputeFirstPosFreq(List<JAT> bucketBoundaries,
	    Map<JAT, FirstPosFreq> firstPosFreq) {
	for (int i = 0; i < bucketBoundaries.size(); i++) {
	    JAT boundary = bucketBoundaries.get(i);
	    if (firstPosFreq.containsKey(boundary)) {
		firstPosFreq.get(boundary).incrementFreq();
	    } else {
		FirstPosFreq fpf = new FirstPosFreq(i, 1);
		firstPosFreq.put(boundary, fpf);
	    }
	}
    }

    private int findBucket(List<JAT> boundaries,
	    Map<JAT, FirstPosFreq> keyToFirstPosFreq, JAT key) {
	if (keyToFirstPosFreq.containsKey(key)) {
	    // LOG.info("EQUALS entering!" + key);
	    return randomChoose(keyToFirstPosFreq.get(key));
	} else {
	    return findBucketNoBoundary(boundaries, key);
	}
    }

    private int randomChoose(FirstPosFreq firstPosFreq) {
	int firstPos = firstPosFreq.getFirstPos();
	int freq = firstPosFreq.getFreq();
	return firstPos + _rndGen.nextInt(freq);
    }

    private int findBucketNoBoundary(List<JAT> boundaries, JAT key) {
	// special case: right from the upperBoundary
	int lowerBound = 0;
	int upperBound = boundaries.size() - 1;
	JAT upperKey = boundaries.get(upperBound);
	int compareToUpper = key.compareTo(upperKey);
	// we could also write "compareToUpper >0" as equal case is covered in
	// findBucket method
	if (compareToUpper >= 0) {
	    return boundaries.size() - 1;
	}

	upperBound--;// because we will look one position right from the
		     // right-most allowed
	while (lowerBound <= upperBound) {
	    int currentPosition = (lowerBound + upperBound) / 2;
	    JAT leftBoundary = boundaries.get(currentPosition);
	    JAT rightBoundary = boundaries.get(currentPosition + 1);
	    int compareToLeft = key.compareTo(leftBoundary);
	    int compareToRight = key.compareTo(rightBoundary);
	    int compareBoundaries = leftBoundary.compareTo(rightBoundary);
	    if (compareBoundaries == 0) {
		// left and right boundaries are equal
		if (compareToLeft == 0) {
		    throw new RuntimeException(
			    "Should not ask for a boundary value " + key
				    + " in method findBucketNoBoundary!");
		    // return currentPosition + 1;
		} else if (compareToLeft < 0) {
		    upperBound = currentPosition - 1;
		} else {
		    lowerBound = currentPosition + 1;
		}
	    } else {
		// left and right boundaries are not equal
		// we could also write "compareToLeft >0" as equal case is
		// covered in findBucket method
		if (compareToLeft >= 0 && compareToRight < 0) {
		    return currentPosition; // PREVIOUSLY ERRONEOUSLY WAS + 1
		} else if (compareToLeft < 0) {
		    upperBound = currentPosition - 1;
		} else if (compareToRight >= 0) {
		    lowerBound = currentPosition + 1;
		} else {
		    throw new RuntimeException("Developer error!");
		}
	    }
	}

	throw new RuntimeException(
		"EWHSample binarySearch must find a bucket! Problematic key is "
			+ key);
    }

    // only SAMPLE_MATRIX_EDGE_PROB = true
    private static <JAT> void precomputeProbForBoundaryKeys(List<JAT> joinKeys,
	    List<JAT> boundaries, List<Integer> boundaryPositions,
	    Map<JAT, ArrayList<BeginProbBucket>> probForBoundaryKeys) {
	int numBuckets = boundaries.size();
	if (numBuckets == 1) {
	    throw new RuntimeException(
		    "This method was not assumed to work for #buckets = 1!");
	}
	int beginBoundaryIndex = 0;
	JAT currentKey = boundaries.get(beginBoundaryIndex);
	int endBoundaryIndex = beginBoundaryIndex + 1;
	// for only one bucket we do nothing
	while (endBoundaryIndex < numBuckets) {
	    for (; endBoundaryIndex < numBuckets; endBoundaryIndex++) {
		JAT endBoundaryKey = boundaries.get(endBoundaryIndex);
		if (!currentKey.equals(endBoundaryKey)) {
		    break;
		}
	    }
	    // the key begins after (or at) beginBoundaryPosition, and ends at
	    // endBoundaryPosition (not including it)
	    // endBoundaryPosition can be = numBuckets, which means we have to
	    // go through the entire last bucket
	    assignProbabilities(currentKey, beginBoundaryIndex,
		    endBoundaryIndex, joinKeys, boundaries, boundaryPositions,
		    probForBoundaryKeys);
	    // the end of the currentKey is before endBoundaryPosition, so we
	    // have to start on endBoundaryPosition - 1 not to miss sth
	    beginBoundaryIndex = endBoundaryIndex - 1;
	    if (endBoundaryIndex < numBuckets) {
		// we don't want to get a nullPointerException
		currentKey = boundaries.get(endBoundaryIndex);
	    }
	}
    }

    // the key begins after (or at) beginBoundaryPosition, and ends at
    // endBoundaryPosition (not including it)
    // endBoundaryPosition can be = numBuckets, which means we have to go
    // through the entire last bucket
    private static <JAT> void assignProbabilities(JAT desiredKey,
	    int beginBoundaryIndex, int endBoundaryIndex, List<JAT> joinKeys,
	    List<JAT> boundaries, List<Integer> boundaryPositions,
	    Map<JAT, ArrayList<BeginProbBucket>> probForBoundaryKeys) {
	if (probForBoundaryKeys.containsKey(desiredKey)) {
	    throw new RuntimeException("Developer error: DesiredKey "
		    + desiredKey + " must be added only once!");
	}
	int numCoveredBuckets = endBoundaryIndex - beginBoundaryIndex;

	// desiredKey is a continuous segment in joinKeys somewhere at
	// [beginKeysPosition, endKeysPosition)
	int numBuckets = boundaries.size();
	int beginKeysPosition = boundaryPositions.get(beginBoundaryIndex);
	int endKeysPosition = -1;
	if (endBoundaryIndex < numBuckets) {
	    endKeysPosition = boundaryPositions.get(endBoundaryIndex);
	} else {
	    endKeysPosition = joinKeys.size();
	}

	// after the next code segment, desiredKey is exactly at
	// [beginKeysPosition, endKeysPosition)
	for (; beginKeysPosition < endKeysPosition; beginKeysPosition++) {
	    JAT current = joinKeys.get(beginKeysPosition);
	    if (current.equals(desiredKey)) {
		break;
	    }
	}
	for (; endKeysPosition > beginKeysPosition; endKeysPosition--) {
	    JAT current = joinKeys.get(endKeysPosition - 1);
	    if (current.equals(desiredKey)) {
		break;
	    }
	}

	// computing the probabilities
	int totalNumPositions = endKeysPosition - beginKeysPosition;
	double startPercentage = 0;
	ArrayList<BeginProbBucket> bpbs = new ArrayList<BeginProbBucket>();
	for (int i = beginBoundaryIndex; i < endBoundaryIndex; i++) {
	    // the bucket positions in joinKeys are [bucketStartKeyPosition,
	    // bucketEndKeyPosition)
	    int bucketStartKeysPosition = boundaryPositions.get(i);
	    int bucketEndKeysPosition = -1;
	    if (i < numBuckets - 1) {
		bucketEndKeysPosition = boundaryPositions.get(i + 1);
	    } else {
		bucketEndKeysPosition = joinKeys.size();
	    }

	    int bucketKeyOverlaps = getOverlaps(beginKeysPosition,
		    endKeysPosition, bucketStartKeysPosition,
		    bucketEndKeysPosition);
	    if (bucketKeyOverlaps > 0) {
		BeginProbBucket bpb = new BeginProbBucket(startPercentage, i);
		bpbs.add(bpb);
	    }

	    double currentPercentage = ((double) bucketKeyOverlaps)
		    / totalNumPositions;
	    startPercentage += currentPercentage;
	}
	if (bpbs.size() != numCoveredBuckets
		&& bpbs.size() != numCoveredBuckets - 1) {
	    throw new RuntimeException(
		    "Developer error: the bpbs arraylist size should be equal to the number of covered buckets!");
	}
	probForBoundaryKeys.put(desiredKey, bpbs);
    }

    // how much intersection between [begin1, end1) and [begin2, end2)
    private static int getOverlaps(int begin1, int end1, int begin2, int end2) {
	int maxBegin = MyUtilities.getMax(begin1, begin2);
	int minEnd = MyUtilities.getMin(end1, end2);
	return minEnd - maxBegin;
    }

    private int findBucket(JAT key, List<JAT> boundaries,
	    Map<JAT, ArrayList<BeginProbBucket>> probForBoundaryKeys) {
	if (probForBoundaryKeys.containsKey(key)) {
	    // LOG.info("EQUALS entering!" + key);
	    double rnd = _rndGen.nextDouble();
	    return randomChoose(probForBoundaryKeys.get(key), rnd);
	} else {
	    return findBucketNoBoundary(boundaries, key);
	}
    }

    private static int randomChoose(ArrayList<BeginProbBucket> keyProbs,
	    double rnd) {
	int lowerPos = 0;
	int upperPos = keyProbs.size() - 1;
	while (lowerPos <= upperPos) {
	    int currentPos = (lowerPos + upperPos) / 2;
	    BeginProbBucket current = keyProbs.get(currentPos);
	    double currentProb = current.getBeginProbability();
	    if (rnd >= currentProb) {
		// either this element, or we go right
		if (currentPos == keyProbs.size() - 1) {
		    // the last bucket: return it
		    return current.getBucketPosition();
		} else {
		    BeginProbBucket next = keyProbs.get(currentPos + 1);
		    double nextProb = next.getBeginProbability();
		    if (rnd < nextProb) {
			// found it, because the starting probability of next
			// bucket is bigger than rnd
			return current.getBucketPosition();
		    } else {
			// let's go right
			lowerPos = currentPos + 1;
		    }
		}
	    } else {
		// let's go left
		upperPos = currentPos - 1;
	    }
	}
	throw new RuntimeException(
		"Developer error: should be able to find an element!");
    }

    // END OF: only SAMPLE_MATRIX_EDGE_PROB = true

    private void scaleOutput(JoinMatrix<JAT> joinMatrix) {
	int bigNumOfBuckets, bigRelationSize, totalOutputs = 0;
	if (_xRelationSize >= _yRelationSize) {
	    bigRelationSize = _xRelationSize;
	    bigNumOfBuckets = _xNumOfBuckets;
	} else {
	    bigRelationSize = _yRelationSize;
	    bigNumOfBuckets = _yNumOfBuckets;
	}

	List<TwoInteger> tis = new ArrayList<TwoInteger>();
	Iterator<long[]> coordinates = joinMatrix
		.getNonEmptyCoordinatesIterator();
	while (coordinates.hasNext()) {
	    long[] coordinate = coordinates.next();
	    int x = (int) coordinate[0];
	    int y = (int) coordinate[1];
	    TwoInteger ti = new TwoInteger(x, y);
	    tis.add(ti);
	}

	double scale = (((double) _computedTotalOutputSize) / _outputSample
		.size()) / ((double) bigRelationSize / bigNumOfBuckets);
	LOG.info("Scaling output by a factor of " + scale);
	int lastBucketOutputSamples = joinMatrix.getElement(
		joinMatrix.getXSize() - 1, joinMatrix.getYSize() - 1);
	LOG.info("Last bucket contains " + lastBucketOutputSamples
		+ " output sample tuples.");
	for (TwoInteger ti : tis) {
	    int x = ti.getX();
	    int y = ti.getY();
	    int element = joinMatrix.getElement(x, y);
	    double scaledElementDbl = scale * element;
	    int scaledElement = (int) (scaledElementDbl + 0.5);
	    // if(scaledElement > 0){
	    // LOG.info("Scaled element is " + scaledElement);
	    // }
	    joinMatrix.setElement(scaledElement, x, y);
	    totalOutputs += scaledElement;
	}
	joinMatrix.setTotalNumOutput(totalOutputs);
    }

    private class TwoInteger {
	private int _x, _y;

	public TwoInteger(int x, int y) {
	    _x = x;
	    _y = y;
	}

	public int getX() {
	    return _x;
	}

	public int getY() {
	    return _y;
	}
    }

    private class TwoString {
	private String _x, _y;

	public TwoString(String x, String y) {
	    _x = x;
	    _y = y;
	}

	public TwoString(List<String> tuple) {
	    _x = tuple.get(0);
	    _y = tuple.get(1);
	}

	public String getX() {
	    return _x;
	}

	public String getY() {
	    return _y;
	}
    }

    private class FirstPosFreq {
	private int _firstPos, _freq;

	public FirstPosFreq(int firstPos, int freq) {
	    _firstPos = firstPos;
	    _freq = freq;
	}

	public void incrementFreq() {
	    _freq++;
	}

	public int getFirstPos() {
	    return _firstPos;
	}

	public int getFreq() {
	    return _freq;
	}
    }

    // used only when SAMPLE_MATRIX_EDGE_PROB = true
    private static class BeginProbBucket {
	private double _beginProbability;
	private int _bucketPosition;

	public BeginProbBucket(double beginProbability, int bucketPosition) {
	    _beginProbability = beginProbability;
	    _bucketPosition = bucketPosition;
	}

	public double getBeginProbability() {
	    return _beginProbability;
	}

	public int getBucketPosition() {
	    return _bucketPosition;
	}

	@Override
	public String toString() {
	    return "BeginProbability = " + _beginProbability
		    + ": bucketPosition = " + _bucketPosition;
	}
    }

    // some testing
    public static void main(String[] args) {
	// DONE: to test, you need to specify static<JAT>
	// precomputeProbForBoundaryKey

	// given
	List<Integer> joinKeys = new ArrayList<Integer>(Arrays.asList(1, 2, 2,
		2, 2, 5, 6));
	List<Integer> boundaryPositions = new ArrayList<Integer>(Arrays.asList(
		0, 1, 4, 5, 6));
	List<Integer> boundaries = new ArrayList<Integer>();
	Map<Integer, ArrayList<BeginProbBucket>> probForBoundaryKeys = new HashMap<Integer, ArrayList<BeginProbBucket>>();

	// derived
	for (Integer boundaryPosition : boundaryPositions) {
	    boundaries.add(joinKeys.get(boundaryPosition));
	}

	// method invocation
	EWHSampleMatrixBolt.precomputeProbForBoundaryKeys(joinKeys, boundaries,
		boundaryPositions, probForBoundaryKeys);
	System.out.println("Result is " + probForBoundaryKeys);

	Integer key1 = 2;
	System.out.println("Key "
		+ key1
		+ " is found in "
		+ EWHSampleMatrixBolt.randomChoose(
			probForBoundaryKeys.get(key1), 0.70));
    }
}