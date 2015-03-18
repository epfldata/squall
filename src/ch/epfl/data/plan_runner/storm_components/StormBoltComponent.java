package ch.epfl.data.plan_runner.storm_components;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import ch.epfl.data.plan_runner.components.ComponentProperties;
import ch.epfl.data.plan_runner.components.JoinerComponent;
import ch.epfl.data.plan_runner.ewh.main.PushStatisticCollector;
import ch.epfl.data.plan_runner.ewh.operators.SampleAsideAndForwardOperator;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.operators.Operator;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.PeriodicAggBatchSend;
import ch.epfl.data.plan_runner.utilities.SystemParameters;
import ch.epfl.data.plan_runner.window_semantics.WindowSemanticsManager;

public abstract class StormBoltComponent extends BaseRichBolt implements
		StormEmitter, StormComponent {

	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormBoltComponent.class);

	private final Map _conf;
	private OutputCollector _collector;
	private final String _ID;
	private final String _componentIndex; // a unique index in a list of all the
	// components
	// used as a shorter name, to save some network traffic
	// it's of type int, but we use String to save more space
	private final boolean _printOut;
	private final int _hierarchyPosition;
	private final StormEmitter[] _parentEmitters;

	// for No ACK: the total number of tasks of all the parent components
	private int _numRemainingParents;

	private final List<Integer> _hashIndexes;
	private final List<ValueExpression> _hashExpressions;

	// printing statistics for graphs
	protected int _thisTaskID;

	// for ManualBatch(Queuing) mode
	private List<Integer> _targetTaskIds;
	private int _targetParallelism;
	private StringBuilder[] _targetBuffers;
	private long[] _targetTimestamps;

	// for CustomTimestamp mode
	private double _totalLatencyMillis;
	private long _numberOfSamples;

	// counting negative values
	protected long numNegatives = 0;
	protected double maxNegative = 0;

	// StatisticsCollector
	private PushStatisticCollector _sc;

	// EWH histogram
	private boolean _isEWHPartitioner;

	/*
	 * //TODO For Window Semantics
	 */
	// //////////////////////////////
	// protected static int WINDOW_SIZE_MULTIPLE_CONSTANT=3;
	public boolean _isLocalWindowSemantics =false;
	public long _windowSize = -1; // Width in terms of millis, Default is -1 which is full history
	public long _latestTimeStamp = -1;
	public long _tumblingWindowSize =-1;//For tumbling semantics

	public StormBoltComponent(ComponentProperties cp,
			List<String> allCompNames, int hierarchyPosition,
			boolean isEWHPartitioner, Map conf) {
		this(cp, allCompNames, hierarchyPosition, conf);
		_isEWHPartitioner = isEWHPartitioner;
	}

	public StormBoltComponent(ComponentProperties cp,
			List<String> allCompNames, int hierarchyPosition, Map conf) {
		_conf = conf;
		_ID = cp.getName();
		_componentIndex = String.valueOf(allCompNames.indexOf(_ID));
		_printOut = cp.getPrintOut();
		_hierarchyPosition = hierarchyPosition;
		_parentEmitters = cp.getParents();
		_hashIndexes = cp.getHashIndexes();
		_hashExpressions = cp.getHashExpressions();
		//setWindowSemantics(conf); // Set Window Semantics if Available in the
									// configuration file
	}

	// ManualBatchMode
	private void addToManualBatch(List<String> tuple, long timestamp) {
		final String tupleHash = MyUtilities.createHashString(tuple,
				_hashIndexes, _hashExpressions, _conf);
		final int dstIndex = MyUtilities.chooseHashTargetIndex(tupleHash,
				_targetParallelism);

		// we put in queueTuple based on tupleHash
		// the same hash is used in BatchStreamGrouping for deciding where a
		// particular targetBuffer is to be sent
		final String tupleString = MyUtilities.tupleToString(tuple, _conf);

		if (MyUtilities.isCustomTimestampMode(_conf))
			if (_targetBuffers[dstIndex].length() == 0)
				// timestamp of the first tuple being added to a buffer is the
				// timestamp of the buffer
				_targetTimestamps[dstIndex] = timestamp;
			else
				// on a bolt, tuples might arrive out of order wrt timestamps
				_targetTimestamps[dstIndex] = MyUtilities.getMin(timestamp,
						_targetTimestamps[dstIndex]);
		_targetBuffers[dstIndex].append(tupleHash)
				.append(SystemParameters.MANUAL_BATCH_HASH_DELIMITER)
				.append(tupleString)
				.append(SystemParameters.MANUAL_BATCH_TUPLE_DELIMITER);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if (_hierarchyPosition == FINAL_COMPONENT) { // then its an intermediate
			// stage not the final
			// one
			if (!MyUtilities.isAckEveryTuple(_conf))
				declarer.declareStream(SystemParameters.EOF_STREAM, new Fields(
						SystemParameters.EOF));
		} else {
			final List<String> outputFields = new ArrayList<String>();
			if (MyUtilities.isManualBatchingMode(_conf)) {
				outputFields.add(StormComponent.COMP_INDEX);
				outputFields.add(StormComponent.TUPLE); // string
			} else {
				outputFields.add(StormComponent.COMP_INDEX);
				outputFields.add(StormComponent.TUPLE); // list of string
				outputFields.add(StormComponent.HASH);
			}
			if (MyUtilities.isCustomTimestampMode(_conf)
					|| MyUtilities.isWindowTimestampMode(_conf))
				outputFields.add(StormComponent.TIMESTAMP);
			declarer.declareStream(SystemParameters.DATA_STREAM, new Fields(
					outputFields));

			if (_isEWHPartitioner) {
				// EQUI-WEIGHT HISTOGRAM
				final List<String> outputFieldsPart = new ArrayList<String>();
				outputFieldsPart.add(StormComponent.COMP_INDEX);
				outputFieldsPart.add(StormComponent.TUPLE); // list of string
				outputFieldsPart.add(StormComponent.HASH);
				declarer.declareStream(SystemParameters.PARTITIONER,
						new Fields(outputFieldsPart));
			}
		}
	}

	protected void finalizeProcessing() {
		printStatistics(SystemParameters.FINAL_PRINT);
		if (getChainOperator() != null) {
			getChainOperator().finalizeProcessing();
		}
		if (MyUtilities.isStatisticsCollector(_conf, _hierarchyPosition)) {
			_sc.finalizeProcessing();
		}
	}

	public abstract ChainOperator getChainOperator();

	public OutputCollector getCollector() {
		return _collector;
	}

	public Map getConf() {
		return _conf;
	}

	// StormEmitter interface
	@Override
	public String[] getEmitterIDs() {
		return new String[] { _ID };
	}

	public int getHierarchyPosition() {
		return _hierarchyPosition;
	}

	// StormComponent
	@Override
	public String getID() {
		return _ID;
	}

	protected abstract InterchangingComponent getInterComp();

	@Override
	public String getName() {
		return _ID;
	}

	public abstract long getNumSentTuples();

	public abstract PeriodicAggBatchSend getPeriodicAggBatch();

	protected void manualBatchSend() {
		for (int i = 0; i < _targetParallelism; i++) {
			final String tupleString = _targetBuffers[i].toString();
			_targetBuffers[i] = new StringBuilder("");

			if (!tupleString.isEmpty())
				// some buffers might be empty
				if (MyUtilities.isCustomTimestampMode(_conf)
						|| MyUtilities.isWindowTimestampMode(_conf))
					_collector.emit(new Values(_componentIndex, tupleString,
							_targetTimestamps[i]));
				else
					_collector.emit(new Values(_componentIndex, tupleString));
		}
	}

	// BaseRichSpout
	@Override
	public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
		setCollector(collector);
		if (getInterComp() == null)
			_numRemainingParents = MyUtilities.getNumParentTasks(tc,
					Arrays.asList(_parentEmitters));
		else
			_numRemainingParents = MyUtilities.getNumParentTasks(tc,
					getInterComp());

		_thisTaskID = tc.getThisTaskId();

		_targetTaskIds = MyUtilities.findTargetTaskIds(tc);
		_targetParallelism = _targetTaskIds.size();
		_targetBuffers = new StringBuilder[_targetParallelism];
		_targetTimestamps = new long[_targetParallelism];
		for (int i = 0; i < _targetParallelism; i++)
			_targetBuffers[i] = new StringBuilder("");

		// initial statistics
		printStatistics(SystemParameters.INITIAL_PRINT);
		if (MyUtilities.isStatisticsCollector(_conf, _hierarchyPosition)) {
			_sc = new PushStatisticCollector(map);
		}

		// equi-weight histogram
		if (_isEWHPartitioner) {
			// extract sampleAside operator
			SampleAsideAndForwardOperator saf = getChainOperator()
					.getSampleAside();
			saf.setCollector(_collector);
			saf.setComponentIndex(_componentIndex);
		}
	}

	@Override
	public void printContent() {
		if (_printOut)
			if ((getChainOperator() != null) && getChainOperator().isBlocking()) {
				final Operator lastOperator = getChainOperator()
						.getLastOperator();
				if (lastOperator instanceof AggregateOperator)
					MyUtilities.printBlockingResult(_ID,
							(AggregateOperator) lastOperator,
							_hierarchyPosition, _conf, LOG);
				else
					MyUtilities.printBlockingResult(_ID,
							lastOperator.getNumTuplesProcessed(),
							lastOperator.printContent(), _hierarchyPosition,
							_conf, LOG);
			}
	}

	protected abstract void printStatistics(int type);

	@Override
	public void printTuple(List<String> tuple) {
		if (_printOut)
			if ((getChainOperator() == null)
					|| !getChainOperator().isBlocking()) {
				final StringBuilder sb = new StringBuilder();
				sb.append("\nComponent ").append(_ID);
				sb.append("\nReceived tuples: ").append(getNumSentTuples());
				sb.append(" Tuple: ").append(
						MyUtilities.tupleToString(tuple, _conf));
				LOG.info(sb.toString());
			}
	}

	// StormComponent
	// tupleSerialNum starts from 0
	@Override
	public void printTupleLatency(long tupleSerialNum, long timestamp) {
		final int freqCompute = SystemParameters.getInt(_conf,
				"FREQ_TUPLE_LOG_COMPUTE");
		final int freqWrite = SystemParameters.getInt(_conf,
				"FREQ_TUPLE_LOG_WRITE");
		final int startupIgnoredTuples = SystemParameters.getInt(_conf,
				"INIT_IGNORED_TUPLES");

		if (tupleSerialNum >= startupIgnoredTuples) {
			tupleSerialNum = tupleSerialNum - startupIgnoredTuples; // start
			// counting
			// from zero
			// when
			// computing
			// starts
			long latencyMillis = -1;
			if (tupleSerialNum % freqCompute == 0) {
				latencyMillis = System.currentTimeMillis() - timestamp;
				if (latencyMillis < 0) {
					numNegatives++;
					if (latencyMillis < maxNegative)
						maxNegative = latencyMillis;
					// for every negative tuple we set 0
					latencyMillis = 0;
				}
				if (_numberOfSamples < 0) {
					LOG.info("Exception! Number of samples is "
							+ _numberOfSamples + "! Ignoring a tuple!");
					return;
				}
				_totalLatencyMillis += latencyMillis;
				_numberOfSamples++;
			}
			if (tupleSerialNum % freqWrite == 0) {
				LOG.info("Taking into account every " + freqCompute
						+ "th tuple, and printing every " + freqWrite
						+ "th one.");
				LOG.info("LAST tuple latency is " + latencyMillis + "ms.");
				LOG.info("AVERAGE tuple latency so far is "
						+ _totalLatencyMillis / _numberOfSamples + "ms.");
			}
		}
	}

	protected void printTupleLatencyFinal() {
		if (_numberOfSamples > 0)
			LOG.info("AVERAGE tuple latency so far is " + _totalLatencyMillis
					/ _numberOfSamples + "ms.");
	}

	// if true, we should exit from method which called this method
	protected boolean processFinalAck(List<String> tuple, Tuple stormTupleRcv) {
		if (MyUtilities.isFinalAck(tuple, getConf())) {
			_numRemainingParents--;
			if (_numRemainingParents == 0) {
				if (MyUtilities.isManualBatchingMode(getConf())) {
					// flushing before sending lastAck down the hierarchy
					manualBatchSend();
				}
				finalizeProcessing();
			}
			MyUtilities.processFinalAck(_numRemainingParents,
					getHierarchyPosition(), getConf(), stormTupleRcv,
					getCollector(), getPeriodicAggBatch());
			if (_isEWHPartitioner) {
				// rel size
				Values relSize = MyUtilities.createRelSizeTuple(
						_componentIndex, (int) getNumSentTuples());
				_collector.emit(SystemParameters.PARTITIONER, relSize);

				// final ack
				MyUtilities.processFinalAckCustomStream(
						SystemParameters.PARTITIONER, _numRemainingParents,
						getHierarchyPosition(), getConf(), stormTupleRcv,
						getCollector(), getPeriodicAggBatch());
			}
			return true;
		}
		return false;
	}

	public abstract void purgeStaleStateFromWindow();

	// //////////////////////////////
	// //////////////////////////////end

	protected boolean receivedDumpSignal(Tuple stormTuple) {
		return stormTuple.getSourceStreamId().equalsIgnoreCase(
				SystemParameters.DUMP_RESULTS_STREAM);
	}

	// non-ManualBatchMode
	private void regularTupleSend(List<String> tuple, Tuple stormTupleRcv,
			long timestamp) {
		final Values stormTupleSnd = MyUtilities.createTupleValues(tuple,
				timestamp, _componentIndex, _hashIndexes, _hashExpressions,
				_conf);
		MyUtilities.sendTuple(stormTupleSnd, stormTupleRcv, _collector, _conf);
	}

	protected void sendToStatisticsCollector(List<String> tuple,
			int relationNumber) {
		if (MyUtilities.isStatisticsCollector(_conf, _hierarchyPosition)) {
			_sc.processTuple(tuple, relationNumber);
		}
	}

	protected void setCollector(OutputCollector collector) {
		_collector = collector;
	}

	// invoked from StormSrcStorage only
	protected void setNumRemainingParents(int numParentTasks) {
		_numRemainingParents = numParentTasks;
	}

	public void setWindowSemantics(long windowSize, long tumblingWindowSize) {
		// Width in terms of millis, Default is -1 which is full history
		_isLocalWindowSemantics=true;
		_windowSize = windowSize;
		_tumblingWindowSize = tumblingWindowSize;//For tumbling semantics
		long max= _windowSize > _tumblingWindowSize? _windowSize: _tumblingWindowSize;
		
		if(_conf.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS)==null){
			_conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, max*2);
			WindowSemanticsManager._GC_PERIODIC_TICK=max*2;
		}
		
		long value =(long) _conf.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS)/2;
				
		if (value > max){
			_conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, value*2);
			WindowSemanticsManager._GC_PERIODIC_TICK=value*2;
		}
		
	}

	@Override
	public void tupleSend(List<String> tuple, Tuple stormTupleRcv,
			long timestamp) {
		if (!MyUtilities.isManualBatchingMode(_conf))
			regularTupleSend(tuple, stormTupleRcv, timestamp);
		else {
			// appending tuple if it is not lastAck
			addToManualBatch(tuple, timestamp);
			if (getNumSentTuples() % MyUtilities.getCompBatchSize(_ID, _conf) == 0)
				manualBatchSend();
		}
	}
}