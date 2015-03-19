package ch.epfl.data.plan_runner.ewh.storm_components;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
import ch.epfl.data.plan_runner.conversion.NumericConversion;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.storm_components.StormComponent;
import ch.epfl.data.plan_runner.storm_components.StormEmitter;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.SystemParameters;
import ch.epfl.data.plan_runner.utilities.SystemParameters.HistogramType;

public class D2CombinerBolt<JAT extends Number & Comparable<JAT>> extends
		BaseRichBolt implements StormEmitter {
	// state
	private enum STATE {
		PRODUCING_D2, PRODUCING_SAMPLE_OUTPUT
	}

	private static final long serialVersionUID = 1L;

	private static Logger LOG = Logger.getLogger(D2CombinerBolt.class);
	private StormEmitter _d2Source;
	private String _d2SourceIndex, _s1ReservoirMergeIndex = null,
			_componentIndex;
	private final String _componentName;
	private NumericConversion _wrapper;
	private ComparisonPredicate _comparison;
	private Map _conf;

	private OutputCollector _collector;

	private List<String> _allCompNames;
	private boolean _isFirstD2;
	private int _hierarchyPosition;
	private int _firstNumOfBuckets, _secondNumOfBuckets;
	private int _numRemainingParents;
	private int _numOfLastJoiners; // input to tiling algorithm

	private boolean _isEWHS1Histogram; // if true, only d2_equi is computed in
										// D2CombinerBolt
	private Map<JAT, Integer> _d2 = new HashMap<JAT, Integer>(); // key,
																	// multiplicity

	private Map<JAT, Integer> _d2equi = new HashMap<JAT, Integer>(); // key,
																		// multiplicity

	private int _r2NumReceivedTuples;

	private Random _rndGen = new Random();;
	private STATE _state = STATE.PRODUCING_D2;

	public D2CombinerBolt(
			StormEmitter d2Source,
			String s1ReservoirMergeName,
			String componentName,
			boolean isFirstD2,
			NumericConversion<JAT> wrapper,
			ComparisonPredicate comparison,
			boolean isEWHS1Histogram, // if true, only d2_equi is computed in
										// D2CombinerBolt
			int firstNumOfBuckets, int secondNumOfBuckets,
			List<String> allCompNames, int hierarchyPosition,
			TopologyBuilder builder, TopologyKiller killer, Config conf) {

		_d2Source = d2Source;
		_d2SourceIndex = String
				.valueOf(allCompNames.indexOf(d2Source.getName()));
		_s1ReservoirMergeIndex = String.valueOf(allCompNames
				.indexOf(s1ReservoirMergeName));
		_componentName = componentName;
		_componentIndex = String.valueOf(allCompNames.indexOf(componentName));

		_isFirstD2 = isFirstD2;
		_allCompNames = allCompNames;
		_hierarchyPosition = hierarchyPosition;
		_conf = conf;
		_comparison = comparison;
		_wrapper = wrapper;
		_isEWHS1Histogram = isEWHS1Histogram;

		_firstNumOfBuckets = firstNumOfBuckets;
		_secondNumOfBuckets = secondNumOfBuckets;

		final int parallelism = SystemParameters.getInt(conf, componentName
				+ "_PAR");

		// connecting with previous level
		InputDeclarer currentBolt = builder.setBolt(componentName, this,
				parallelism);

		HistogramType histType = null;
		if (SystemParameters.getBooleanIfExist(_conf,
				HistogramType.D2_COMB_HIST.readConfEntryName())) {
			histType = HistogramType.D2_COMB_HIST;
		}

		currentBolt = MyUtilities.attachEmitterRangeMulticast(conf, comparison,
				_wrapper, histType, currentBolt, d2Source);
		currentBolt = MyUtilities.attachEmitterRange(_conf, _wrapper, histType,
				currentBolt, s1ReservoirMergeName);

		if (_hierarchyPosition == StormComponent.FINAL_COMPONENT) {
			killer.registerComponent(this, componentName, parallelism);
		}
	}

	private void addMultiplicity(Map<JAT, Integer> stats, JAT key, int mult) {
		if (stats.containsKey(key)) {
			mult += stats.get(key);
		}
		stats.put(key, mult);
	}

	// increasing the multiplicity for each key which is joinable with key (arg)
	// from the opposite relations
	private void addMultiplicityJoin(Map<JAT, Integer> stats, JAT key, int mult) {
		List<JAT> joinableKeys = _comparison.getJoinableKeys(key);
		for (JAT joinableKey : joinableKeys) {
			addMultiplicity(stats, joinableKey, mult);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if (_hierarchyPosition == StormComponent.FINAL_COMPONENT) {
			declarer.declareStream(SystemParameters.EOF_STREAM, new Fields(
					SystemParameters.EOF));
		} else {
			// stream for sending the d2 data to s1 component
			final List<String> outputFields = new ArrayList<String>();
			outputFields.add(StormComponent.COMP_INDEX);
			outputFields.add(StormComponent.TUPLE); // list of string
			outputFields.add(StormComponent.HASH);
			declarer.declareStream(SystemParameters.D2_TO_S1_STREAM,
					new Fields(outputFields));

			// default stream (used only for ACKS) ; used only for S1Reservoir
			// component
			// the reason to avoid DATA_STREAM is to allow to send EOF to
			// different children at different moment
			final List<String> outputFieldsDef = new ArrayList<String>();
			outputFieldsDef.add(StormComponent.COMP_INDEX);
			outputFieldsDef.add(StormComponent.TUPLE); // list of string
			outputFieldsDef.add(StormComponent.HASH);
			declarer.declareStream(SystemParameters.DATA_STREAM, new Fields(
					outputFieldsDef));

			// stream for sending Output Sample Tuples to the partitioner
			final List<String> outputFieldsPart = new ArrayList<String>();
			outputFieldsPart.add(StormComponent.COMP_INDEX);
			outputFieldsPart.add(StormComponent.TUPLE); // list of string
			outputFieldsPart.add(StormComponent.HASH);
			declarer.declareStream(SystemParameters.PARTITIONER, new Fields(
					outputFieldsPart));

			// this is the last component in this cyclic topology
			declarer.declareStream(SystemParameters.EOF_STREAM, new Fields(
					SystemParameters.EOF));
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

	private void finalizeProcessing() {
		if (_state == STATE.PRODUCING_D2) {
			if (SystemParameters.getBooleanIfExist(_conf, "D2_DEBUG_MODE")) {
				// this is just for local debugging
				LOG.info("d2equi in D2Combiner is " + _d2equi);
				LOG.info("d2 in D2Combiner is " + _d2);
			}
			LOG.info("Received _r2NumReceivedTuples = " + _r2NumReceivedTuples
					+ " tuples.");
			LOG.info("Received all the tuples from R2. Sending tuples to S1Reservoir...");

			// either d2 or d2equi send to S1ReduceBolt
			Map<JAT, Integer> mapToSend;
			if (!_isEWHS1Histogram) {
				mapToSend = _d2;
			} else {
				mapToSend = _d2equi;
			}

			for (Map.Entry<JAT, Integer> entry : mapToSend.entrySet()) {
				JAT key = entry.getKey();
				String strKey = MyUtilities.toSpecialString(key, _wrapper);

				int mult = entry.getValue();
				String strMult = String.valueOf(mult);

				List<String> tuple = new ArrayList<String>(Arrays.asList(
						strKey, strMult));
				List<Integer> hashIndexes = new ArrayList<Integer>(
						Arrays.asList(0));
				tupleSend(SystemParameters.D2_TO_S1_STREAM, tuple, hashIndexes);
			}
			LOG.info("All tuples sent to S1Reservoir. Moving to PRODUCING_SAMPLE_OUTPUT state.");
		} else if (_state == STATE.PRODUCING_SAMPLE_OUTPUT) {
			// output sample is produced in an online fashion
			// nothing to do
			LOG.info("Finished with PRODUCING_SAMPLE_OUTPUT state. All the tuples sent to Partitioner");
		} else {
			throw new RuntimeException("Should not be here!");
		}
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
	public String getInfoID() {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public String getName() {
		return _componentName;
	}

	private JAT getOppositeKey(JAT s1Key) {
		List<JAT> joinableKeys = _comparison.getJoinableKeys(s1Key);

		// these collections are of the same size; contains keys with
		// multiplicity > 0
		List<JAT> joinableKeysWithMult = new ArrayList<JAT>();
		List<Integer> multiplicities = new ArrayList<Integer>();
		List<Double> probabilities = new ArrayList<Double>();
		int totalMultiplicity = 0;

		// LOG.info("New tuple");
		// first pass to get the totalMultiplicity
		for (JAT joinableKey : joinableKeys) {
			if (_d2equi.containsKey(joinableKey)) {
				joinableKeysWithMult.add(joinableKey);
				int multiplicity = _d2equi.get(joinableKey);
				// LOG.info("multiplicity is " + multiplicity);
				multiplicities.add(multiplicity);
				totalMultiplicity += multiplicity;
			}
		}
		// LOG.info("Total multiplicity is " + totalMultiplicity);

		// second pass to compute probabilities
		// each joinable key has a probability to be included
		// [previous.probabilityEnd, probabilityEnd)
		int multiplicitySum = 0;
		for (int multiplicity : multiplicities) {
			multiplicitySum += multiplicity;
			double probabilityEnd = ((double) (multiplicitySum))
					/ totalMultiplicity;
			probabilities.add(probabilityEnd);
		}

		// choose one tuple among the joinable ones proportionally to their
		// multiplicity
		double random = _rndGen.nextDouble();
		// LOG.info("Random value is " + random);
		int index = -1;
		double probabilityBeg = 0;
		for (int i = 0; i < probabilities.size(); i++) {
			double probabilityEnd = probabilities.get(i);
			// LOG.info("Probability range is [" + probabilityBeg + ", " +
			// probabilityEnd + ")");
			if (random >= probabilityBeg && random < probabilityEnd) {
				index = i;
				break;
			}
			probabilityBeg = probabilityEnd;
		}

		return joinableKeysWithMult.get(index);
	}

	// BaseRichSpout
	@Override
	public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
		_collector = collector;
		_numRemainingParents = MyUtilities.getNumParentTasks(tc,
				Arrays.asList(_d2Source));
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

			// process final ack according to the state
			if (_state == STATE.PRODUCING_D2) {
				MyUtilities.processFinalAck(_numRemainingParents,
						StormComponent.INTERMEDIATE, _conf, stormTupleRcv,
						_collector);
			} else if (_state == STATE.PRODUCING_SAMPLE_OUTPUT) {
				// MyUtilities.processFinalAck(_numRemainingParents,
				// StormComponent.FINAL_COMPONENT, _conf,
				// stormTupleRcv, _collector);
				MyUtilities.processFinalAckCustomStream(
						SystemParameters.PARTITIONER, _numRemainingParents,
						StormComponent.INTERMEDIATE, _conf, stormTupleRcv,
						_collector);
			} else {
				throw new RuntimeException("Should not be here!");
			}

			// advancing the state
			if (_state == STATE.PRODUCING_D2 && _numRemainingParents == 0) {
				_state = STATE.PRODUCING_SAMPLE_OUTPUT;
				_numRemainingParents = 1; // _s1ReservoirMerge_PAR = 1 by design
			}

			return true;
		}
		return false;
	}

	private void processNonLastTuple(String inputComponentIndex,
			String sourceStreamId, List<String> tuple) {
		if (inputComponentIndex.equals(_d2SourceIndex)) {
			_r2NumReceivedTuples++;
			String strKey = tuple.get(0); // key is the only thing sent
			JAT key = (JAT) _wrapper.fromString(strKey);
			addMultiplicity(_d2equi, key, 1);
			if (!_isEWHS1Histogram) {
				addMultiplicityJoin(_d2, key, 1);
			}
		} else if (inputComponentIndex.equals(_s1ReservoirMergeIndex)) {
			// s1
			String strS1Key = tuple.get(0);
			JAT s1Key = (JAT) _wrapper.fromString(strS1Key);
			// opposite
			JAT oppositeKey = getOppositeKey(s1Key);
			String strOppositeKey = MyUtilities.toSpecialString(oppositeKey,
					_wrapper);
			// output
			List<String> outputSampleTuple;
			if (_isFirstD2) {
				outputSampleTuple = new ArrayList<String>(Arrays.asList(
						strOppositeKey, strS1Key));
			} else {
				outputSampleTuple = new ArrayList<String>(Arrays.asList(
						strS1Key, strOppositeKey));
			}
			if (SystemParameters.getBooleanIfExist(_conf, "DEBUG_MODE")) {
				// this is just for local debugging
				LOG.info("Produced output sample tuple " + outputSampleTuple);
			}

			// let's send it
			List<Integer> hashIndexes = new ArrayList<Integer>(Arrays.asList(0)); // does
																					// not
																					// matter
			tupleSend(SystemParameters.PARTITIONER, outputSampleTuple,
					hashIndexes);
		} else {
			throw new RuntimeException("Unsupported source component index "
					+ inputComponentIndex);
		}
	}

	// sending with the default stream
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
}