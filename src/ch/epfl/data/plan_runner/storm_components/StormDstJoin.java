package ch.epfl.data.plan_runner.storm_components;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import ch.epfl.data.plan_runner.components.ComponentProperties;
import ch.epfl.data.plan_runner.components.JoinerComponent;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.operators.Operator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.storage.AggregationStorage;
import ch.epfl.data.plan_runner.storage.BasicStore;
import ch.epfl.data.plan_runner.storage.KeyValueStore;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.PeriodicAggBatchSend;
import ch.epfl.data.plan_runner.utilities.SystemParameters;
import ch.epfl.data.plan_runner.utilities.statistics.StatisticsUtilities;
import ch.epfl.data.plan_runner.window_semantics.WindowSemanticsManager;

@Deprecated
public class StormDstJoin extends StormBoltComponent {

	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormDstJoin.class);

	private final String _firstEmitterIndex, _secondEmitterIndex;

	private final BasicStore<ArrayList<String>> _firstRelationStorage,
			_secondRelationStorage;
	private final ProjectOperator _firstPreAggProj, _secondPreAggProj; // exists
																		// only
	// for
	// preaggregations
	// performed on the output of the aggregationStorage

	private final ChainOperator _operatorChain;
	private final List<Integer> _rightHashIndexes; // hash indexes from the
													// right
	// parent

	private long _numSentTuples = 0;

	// for load-balancing
	private final List<String> _fullHashList;
	private String _name;

	// for batch sending
	private final Semaphore _semAgg = new Semaphore(1, true);
	private boolean _firstTime = true;
	private PeriodicAggBatchSend _periodicAggBatch;
	private final long _aggBatchOutputMillis;
	private boolean _isRemoveIndex;

	// for printing statistics for creating graphs
	protected Calendar _cal = Calendar.getInstance();
	protected DateFormat _statDateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
	protected StatisticsUtilities _statsUtils;

	public StormDstJoin(StormEmitter firstEmitter, StormEmitter secondEmitter,
			ComponentProperties cp, List<String> allCompNames,
			BasicStore<ArrayList<String>> firstSquallStorage,
			BasicStore<ArrayList<String>> secondSquallStorage,
			ProjectOperator firstPreAggProj, ProjectOperator secondPreAggProj,
			int hierarchyPosition, TopologyBuilder builder,
			TopologyKiller killer, Config conf, boolean isRemoveIndex) {
		super(cp, allCompNames, hierarchyPosition, conf);

		_firstEmitterIndex = String.valueOf(allCompNames.indexOf(firstEmitter
				.getName()));
		_secondEmitterIndex = String.valueOf(allCompNames.indexOf(secondEmitter
				.getName()));
		_rightHashIndexes = cp.getParents()[1].getHashIndexes();

		_firstRelationStorage = firstSquallStorage;
		_secondRelationStorage = secondSquallStorage;
		_firstPreAggProj = firstPreAggProj;
		_secondPreAggProj = secondPreAggProj;

		_operatorChain = cp.getChainOperator();
		_fullHashList = cp.getFullHashList();

		_aggBatchOutputMillis = cp.getBatchOutputMillis();

		_statsUtils = new StatisticsUtilities(getConf(), LOG);

		_name = cp.getName();

		_isRemoveIndex = isRemoveIndex;

		final int parallelism = SystemParameters.getInt(getConf(), getID()
				+ "_PAR");

		// connecting with previous level
		InputDeclarer currentBolt = builder.setBolt(getID(), this, parallelism);
		if (MyUtilities.isManualBatchingMode(getConf()))
			currentBolt = MyUtilities.attachEmitterBatch(conf, _fullHashList,
					currentBolt, firstEmitter, secondEmitter);
		else
			currentBolt = MyUtilities.attachEmitterHash(conf, _fullHashList,
					currentBolt, firstEmitter, secondEmitter);

		// connecting with Killer
		if (getHierarchyPosition() == FINAL_COMPONENT
				&& (!MyUtilities.isAckEveryTuple(conf)))
			killer.registerComponent(this, parallelism);
		if (cp.getPrintOut() && _operatorChain.isBlocking())
			currentBolt.allGrouping(killer.getID(),
					SystemParameters.DUMP_RESULTS_STREAM);
	}

	@Override
	public void aggBatchSend() {
		if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
			if (_operatorChain != null) {
				final Operator lastOperator = _operatorChain.getLastOperator();
				if (lastOperator instanceof AggregateOperator) {
					try {
						_semAgg.acquire();
					} catch (final InterruptedException ex) {
					}

					// sending
					final AggregateOperator agg = (AggregateOperator) lastOperator;
					final List<String> tuples = agg.getContent();
					if (tuples != null) {
						final String columnDelimiter = MyUtilities
								.getColumnDelimiter(getConf());
						for (String tuple : tuples) {
							tuple = tuple.replaceAll(" = ", columnDelimiter);
							tupleSend(
									MyUtilities.stringToTuple(tuple, getConf()),
									null, 0);
						}
					}

					// clearing
					agg.clearStorage();

					_semAgg.release();
				}
			}
	}

	protected void applyOperatorsAndSend(Tuple stormTupleRcv,
			List<String> tuple, long lineageTimestamp, boolean isLastInBatch) {
		// System.out.println("Seding Out tuple.....");
		if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
			try {
				_semAgg.acquire();
			} catch (final InterruptedException ex) {
			}

		tuple = _operatorChain.process(tuple,0);

		if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
			_semAgg.release();

		if (tuple == null)
			return;
		_numSentTuples++;
		printTuple(tuple);

		if (_numSentTuples % _statsUtils.getDipOutputFreqPrint() == 0)
			printStatistics(SystemParameters.OUTPUT_PRINT);

		if (MyUtilities
				.isSending(getHierarchyPosition(), _aggBatchOutputMillis)
				|| MyUtilities.isWindowTimestampMode(getConf())) {
			long timestamp = 0;
			if (MyUtilities.isCustomTimestampMode(getConf()))
				timestamp = stormTupleRcv
						.getLongByField(StormComponent.TIMESTAMP);
			if (MyUtilities.isWindowTimestampMode(getConf()))
				timestamp = lineageTimestamp;
			tupleSend(tuple, stormTupleRcv, timestamp);
		}
		if (MyUtilities.isPrintLatency(getHierarchyPosition(), getConf())) {
			long timestamp;
			if (MyUtilities.isManualBatchingMode(getConf())) {
				if (isLastInBatch) {
					timestamp = stormTupleRcv
							.getLongByField(StormComponent.TIMESTAMP);
					printTupleLatency(_numSentTuples - 1, timestamp);
				}
			} else {
				timestamp = stormTupleRcv
						.getLongByField(StormComponent.TIMESTAMP);
				printTupleLatency(_numSentTuples - 1, timestamp);
			}
		}
	}

	@Override
	public void execute(Tuple stormTupleRcv) {
		// TODO
		// short circuit that this is a window configuration
		if (WindowSemanticsManager.evictStateIfSlidingWindowSemantics(this,
				stormTupleRcv)) {
			return;
		}

		if (_firstTime
				&& MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis)) {
			_periodicAggBatch = new PeriodicAggBatchSend(_aggBatchOutputMillis,
					this);
			_firstTime = false;
		}

		if (receivedDumpSignal(stormTupleRcv)) {
			MyUtilities.dumpSignal(this, stormTupleRcv, getCollector());
			return;
		}

		if (!MyUtilities.isManualBatchingMode(getConf())) {
			final String inputComponentIndex = stormTupleRcv
					.getStringByField(StormComponent.COMP_INDEX); // getString(0);
			final List<String> tuple = (List<String>) stormTupleRcv
					.getValueByField(StormComponent.TUPLE); // getValue(1);
			final String inputTupleHash = stormTupleRcv
					.getStringByField(StormComponent.HASH);// getString(2);

			if (processFinalAck(tuple, stormTupleRcv))
				return;

			processNonLastTuple(inputComponentIndex, tuple, inputTupleHash,
					stormTupleRcv, true);

		} else {
			final String inputComponentIndex = stormTupleRcv
					.getStringByField(StormComponent.COMP_INDEX); // getString(0);
			final String inputBatch = stormTupleRcv
					.getStringByField(StormComponent.TUPLE);// getString(1);

			final String[] wholeTuples = inputBatch
					.split(SystemParameters.MANUAL_BATCH_TUPLE_DELIMITER);
			final int batchSize = wholeTuples.length;
			for (int i = 0; i < batchSize; i++) {
				// parsing
				final String currentTuple = new String(wholeTuples[i]);
				final String[] parts = currentTuple
						.split(SystemParameters.MANUAL_BATCH_HASH_DELIMITER);

				String inputTupleHash = null;
				String inputTupleString = null;
				if (parts.length == 1)
					// lastAck
					inputTupleString = new String(parts[0]);
				else {
					inputTupleHash = new String(parts[0]);
					inputTupleString = new String(parts[1]);
				}
				final List<String> tuple = MyUtilities.stringToTuple(
						inputTupleString, getConf());

				// final Ack check
				if (processFinalAck(tuple, stormTupleRcv)) {
					if (i != batchSize - 1)
						throw new RuntimeException(
								"Should not be here. LAST_ACK is not the last tuple!");
					return;
				}

				// processing a tuple
				if (i == batchSize - 1)
					processNonLastTuple(inputComponentIndex, tuple,
							inputTupleHash, stormTupleRcv, true);
				else
					processNonLastTuple(inputComponentIndex, tuple,
							inputTupleHash, stormTupleRcv, false);
			}
		}

		// TODO
		// Update LatestTimeStamp
		WindowSemanticsManager.updateLatestTimeStamp(this, stormTupleRcv);
		getCollector().ack(stormTupleRcv);
	}

	@Override
	public ChainOperator getChainOperator() {
		return _operatorChain;
	}

	// from IRichBolt
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return getConf();
	}

	// from StormComponent interface
	@Override
	public String getInfoID() {
		final String str = "DestinationStorage " + getID() + " has ID: "
				+ getID();
		return str;
	}

	@Override
	protected InterchangingComponent getInterComp() {
		// should never be invoked
		return null;
	}

	// HELPER

	@Override
	public long getNumSentTuples() {
		return _numSentTuples;
	}

	@Override
	public PeriodicAggBatchSend getPeriodicAggBatch() {
		return _periodicAggBatch;
	}

	protected void performJoin(Tuple stormTupleRcv, List<String> tuple,
			String inputTupleHash, boolean isFromFirstEmitter,
			BasicStore<ArrayList<String>> oppositeStorage,
			ProjectOperator projPreAgg, boolean isLastInBatch) {

		final List<String> oppositeStringTupleList = oppositeStorage
				.access(inputTupleHash);

		long lineageTimestamp = 0;

		if (oppositeStringTupleList != null)
			for (int i = 0; i < oppositeStringTupleList.size(); i++) {
				// ValueOf is because of preaggregations, and it does not hurt
				// in normal case
				// TODO
				StringBuilder oppositeTupleString = new StringBuilder(
						oppositeStringTupleList.get(i));
				lineageTimestamp = WindowSemanticsManager
						.joinPreProcessingIfSlidingWindowSemantics(this,
								oppositeTupleString, stormTupleRcv);
				if (lineageTimestamp < 0)
					continue;
				// end TODO

				final List<String> oppositeTuple = MyUtilities.stringToTuple(
						oppositeTupleString.toString(),
						getComponentConfiguration());

				List<String> firstTuple, secondTuple;
				if (isFromFirstEmitter) {
					firstTuple = tuple;
					secondTuple = oppositeTuple;
				} else {
					firstTuple = oppositeTuple;
					secondTuple = tuple;
				}

				List<String> outputTuple;
				// Before fixing preaggregations, here was instanceof BasicStore
				if (oppositeStorage instanceof AggregationStorage
						|| !_isRemoveIndex) {
					// preaggregation
					outputTuple = MyUtilities.createOutputTuple(firstTuple,
							secondTuple);
				} else {
					outputTuple = MyUtilities.createOutputTuple(firstTuple,
							secondTuple, _rightHashIndexes);
				}

				if (projPreAgg != null)
					// preaggregation
					outputTuple = projPreAgg.process(outputTuple,0);
				applyOperatorsAndSend(stormTupleRcv, outputTuple,
						lineageTimestamp, isLastInBatch);
			}
	}

	@Override
	protected void printStatistics(int type) {
		if (_statsUtils.isTestMode())
			if (getHierarchyPosition() == StormComponent.FINAL_COMPONENT) {
				// computing variables
				final int size1 = ((KeyValueStore<String, String>) _firstRelationStorage)
						.size();
				final int size2 = ((KeyValueStore<String, String>) _secondRelationStorage)
						.size();
				final int totalSize = size1 + size2;
				final String ts = _statDateFormat.format(_cal.getTime());

				// printing
				if (!MyUtilities.isCustomTimestampMode(getConf())) {
					final Runtime runtime = Runtime.getRuntime();
					final long memory = runtime.totalMemory()
							- runtime.freeMemory();
					if (type == SystemParameters.INITIAL_PRINT)
						LOG.info(","
								+ "INITIAL,"
								+ _thisTaskID
								+ ","
								+ " TimeStamp:,"
								+ ts
								+ ", FirstStorage:,"
								+ size1
								+ ", SecondStorage:,"
								+ size2
								+ ", Total:,"
								+ totalSize
								+ ", Memory used: ,"
								+ StatisticsUtilities.bytesToMegabytes(memory)
								+ ","
								+ StatisticsUtilities.bytesToMegabytes(runtime
										.totalMemory()));
					else if (type == SystemParameters.INPUT_PRINT)
						LOG.info(","
								+ "MEMORY,"
								+ _thisTaskID
								+ ","
								+ " TimeStamp:,"
								+ ts
								+ ", FirstStorage:,"
								+ size1
								+ ", SecondStorage:,"
								+ size2
								+ ", Total:,"
								+ totalSize
								+ ", Memory used: ,"
								+ StatisticsUtilities.bytesToMegabytes(memory)
								+ ","
								+ StatisticsUtilities.bytesToMegabytes(runtime
										.totalMemory()));
					else if (type == SystemParameters.OUTPUT_PRINT)
						LOG.info("," + "RESULT," + _thisTaskID + ","
								+ "TimeStamp:," + ts + ",Sent Tuples,"
								+ getNumSentTuples());
					else if (type == SystemParameters.FINAL_PRINT) {
						if (numNegatives > 0)
							LOG.info("WARNINGLAT! Negative latency for "
									+ numNegatives + ", at most " + maxNegative
									+ "ms.");
						LOG.info(","
								+ "MEMORY,"
								+ _thisTaskID
								+ ","
								+ " TimeStamp:,"
								+ ts
								+ ", FirstStorage:,"
								+ size1
								+ ", SecondStorage:,"
								+ size2
								+ ", Total:,"
								+ totalSize
								+ ", Memory used: ,"
								+ StatisticsUtilities.bytesToMegabytes(memory)
								+ ","
								+ StatisticsUtilities.bytesToMegabytes(runtime
										.totalMemory()));
						LOG.info("," + "RESULT," + _thisTaskID + ","
								+ "TimeStamp:," + ts + ",Sent Tuples,"
								+ getNumSentTuples());
					}
				} else // only final statistics is printed if we are measuring
						// latency
				if (type == SystemParameters.FINAL_PRINT) {
					final Runtime runtime = Runtime.getRuntime();
					final long memory = runtime.totalMemory()
							- runtime.freeMemory();
					if (numNegatives > 0)
						LOG.info("WARNINGLAT! Negative latency for "
								+ numNegatives + ", at most " + maxNegative
								+ "ms.");
					LOG.info(","
							+ "MEMORY,"
							+ _thisTaskID
							+ ","
							+ " TimeStamp:,"
							+ ts
							+ ", FirstStorage:,"
							+ size1
							+ ", SecondStorage:,"
							+ size2
							+ ", Total:,"
							+ totalSize
							+ ", Memory used: ,"
							+ StatisticsUtilities.bytesToMegabytes(memory)
							+ ","
							+ StatisticsUtilities.bytesToMegabytes(runtime
									.totalMemory()));
					LOG.info("," + "RESULT," + _thisTaskID + ","
							+ "TimeStamp:," + ts + ",Sent Tuples,"
							+ getNumSentTuples());
				}
			}
	}

	private void processNonLastTuple(String inputComponentIndex,
			List<String> tuple, String inputTupleHash, Tuple stormTupleRcv,
			boolean isLastInBatch) {

		boolean isFromFirstEmitter = false;
		BasicStore<ArrayList<String>> affectedStorage, oppositeStorage;
		ProjectOperator projPreAgg;
		if (_firstEmitterIndex.equals(inputComponentIndex)) {
			// R update
			isFromFirstEmitter = true;
			affectedStorage = _firstRelationStorage;
			oppositeStorage = _secondRelationStorage;
			projPreAgg = _secondPreAggProj;
		} else if (_secondEmitterIndex.equals(inputComponentIndex)) {
			// S update
			isFromFirstEmitter = false;
			affectedStorage = _secondRelationStorage;
			oppositeStorage = _firstRelationStorage;
			projPreAgg = _firstPreAggProj;
		} else
			throw new RuntimeException("InputComponentName "
					+ inputComponentIndex + " doesn't match neither "
					+ _firstEmitterIndex + " nor " + _secondEmitterIndex + ".");

		// add the stormTuple to the specific storage
		if (affectedStorage instanceof AggregationStorage)
			// For preaggregations, we have to update the storage, not to insert
			// to it
			affectedStorage.update(tuple, inputTupleHash);
		else {
			String inputTupleString = MyUtilities.tupleToString(tuple,
					getConf());
			// TODO
			// add the stormTuple to the specific storage
			inputTupleString = WindowSemanticsManager
					.AddTimeStampToStoredDataIfWindowSemantics(this,
							inputTupleString, stormTupleRcv);
			affectedStorage.insert(inputTupleHash, inputTupleString);
		}
		performJoin(stormTupleRcv, tuple, inputTupleHash, isFromFirstEmitter,
				oppositeStorage, projPreAgg, isLastInBatch);

		if ((((KeyValueStore<String, String>) _firstRelationStorage).size() + ((KeyValueStore<String, String>) _secondRelationStorage)
				.size()) % _statsUtils.getDipInputFreqPrint() == 0)
			printStatistics(SystemParameters.INPUT_PRINT);
	}

	@Override
	public void purgeStaleStateFromWindow() {
		// TODO inefficient linear scan for now.
		System.out.println("Cleaning up state");
		((KeyValueStore<String, String>) _firstRelationStorage)
				.purgeState(_latestTimeStamp - WindowSemanticsManager._GC_PERIODIC_TICK);
		((KeyValueStore<String, String>) _secondRelationStorage)
				.purgeState(_latestTimeStamp - WindowSemanticsManager._GC_PERIODIC_TICK);
		System.gc();
	}

}