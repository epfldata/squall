package plan_runner.storm_components;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import plan_runner.components.ComponentProperties;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.Operator;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.predicates.Predicate;
import plan_runner.storage.BPlusTreeStore;
import plan_runner.storage.BerkeleyDBStore;
import plan_runner.storage.BerkeleyDBStoreSkewed;
import plan_runner.storm_components.synchronization.TopologyKiller;
import plan_runner.thetajoin.matrix_mapping.EquiMatrixAssignment;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.PeriodicAggBatchSend;
import plan_runner.utilities.SystemParameters;
import plan_runner.utilities.statistics.StatisticsUtilities;
import plan_runner.visitors.PredicateCreateIndexesVisitor;
import plan_runner.visitors.PredicateUpdateIndexesVisitor;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;

public class StormThetaJoinBDB extends StormBoltComponent {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormThetaJoinBDB.class);

	private BPlusTreeStore _firstRelationStorage, _secondRelationStorage;
	private final String _firstEmitterIndex, _secondEmitterIndex;

	private long _numSentTuples = 0;

	private final ChainOperator _operatorChain;

	// position to test for equality in first and second emitter
	// join params of current storage then other relation interchangably !!
	List<Integer> _joinParams;

	private final Predicate _joinPredicate;

	private List<Integer> _operatorForIndexes;
	private List<Object> _typeOfValueIndexed;

	private boolean _existIndexes = false;

	// for agg batch sending
	private final Semaphore _semAgg = new Semaphore(1, true);
	private boolean _firstTime = true;
	private PeriodicAggBatchSend _periodicAggBatch;
	private final long _aggBatchOutputMillis;

	private InterchangingComponent _inter = null;

	// for printing statistics for creating graphs
	protected Calendar _cal = Calendar.getInstance();
	protected DateFormat _dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
	protected SimpleDateFormat _format = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy");
	protected StatisticsUtilities _statsUtils;

	public StormThetaJoinBDB(StormEmitter firstEmitter, StormEmitter secondEmitter,
			ComponentProperties cp, List<String> allCompNames, Predicate joinPredicate,
			int hierarchyPosition, TopologyBuilder builder, TopologyKiller killer, Config conf,
			InterchangingComponent interComp) {

		super(cp, allCompNames, hierarchyPosition, conf);

		_firstEmitterIndex = String.valueOf(allCompNames.indexOf(firstEmitter.getName()));
		_secondEmitterIndex = String.valueOf(allCompNames.indexOf(secondEmitter.getName()));
		_aggBatchOutputMillis = cp.getBatchOutputMillis();
		_statsUtils = new StatisticsUtilities(getConf(), LOG);

		final int firstCardinality = SystemParameters
				.getInt(conf, firstEmitter.getName() + "_CARD");
		final int secondCardinality = SystemParameters.getInt(conf, secondEmitter.getName()
				+ "_CARD");

		final int parallelism = SystemParameters.getInt(conf, getID() + "_PAR");

		_operatorChain = cp.getChainOperator();

		_joinPredicate = joinPredicate;

		InputDeclarer currentBolt = builder.setBolt(getID(), this, parallelism);

		final EquiMatrixAssignment _currentMappingAssignment = new EquiMatrixAssignment(
				firstCardinality, secondCardinality, parallelism, -1);
		final String dim = _currentMappingAssignment.getMappingDimensions();
		LOG.info(getID() + " Initial Dimensions is: " + dim);

		if (interComp == null)
			currentBolt = MyUtilities.thetaAttachEmitterComponents(currentBolt, firstEmitter,
					secondEmitter, allCompNames, _currentMappingAssignment, conf);
		else {
			currentBolt = MyUtilities.thetaAttachEmitterComponentsWithInterChanging(currentBolt,
					firstEmitter, secondEmitter, allCompNames, _currentMappingAssignment, conf,
					interComp);
			_inter = interComp;
		}

		if (getHierarchyPosition() == FINAL_COMPONENT && (!MyUtilities.isAckEveryTuple(conf)))
			killer.registerComponent(this, parallelism);
		if (cp.getPrintOut() && _operatorChain.isBlocking())
			currentBolt.allGrouping(killer.getID(), SystemParameters.DUMP_RESULTS_STREAM);
		LOG.info("Storage with BDB!");
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
					for (final String tuple : tuples)
						tupleSend(MyUtilities.stringToTuple(tuple, getConf()), null, 0);
					// clearing
					agg.clearStorage();
					_semAgg.release();
				}
			}
	}

	protected void applyOperatorsAndSend(Tuple stormTupleRcv, List<String> tuple,
			long lineageTimestamp, boolean isLastInBatch) {
		if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
			try {
				_semAgg.acquire();
			} catch (final InterruptedException ex) {
			}
		tuple = _operatorChain.process(tuple);
		if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
			_semAgg.release();

		if (tuple == null)
			return;
		_numSentTuples++;
		printTuple(tuple);

		if (_numSentTuples % _statsUtils.getDipOutputFreqPrint() == 0)
			printStatistics(SystemParameters.OUTPUT_PRINT);

		if (MyUtilities.isSending(getHierarchyPosition(), _aggBatchOutputMillis)) {
			long timestamp = 0;
			if (MyUtilities.isCustomTimestampMode(getConf()))
				if (getHierarchyPosition() == StormComponent.NEXT_TO_LAST_COMPONENT)
					// A tuple has a non-null timestamp only if the component is
					// next to last
					// because we measure the latency of the last operator
					timestamp = System.currentTimeMillis();
			// timestamp = System.nanoTime();
			tupleSend(tuple, stormTupleRcv, timestamp);
		}

		if (MyUtilities.isPrintLatency(getHierarchyPosition(), getConf()))
			printTupleLatency(_numSentTuples - 1, lineageTimestamp);
	}

	private void createStorage() {
		final PredicateCreateIndexesVisitor visitor = new PredicateCreateIndexesVisitor();
		_joinPredicate.accept(visitor);
		_operatorForIndexes = new ArrayList<Integer>(visitor._operatorForIndexes);
		_typeOfValueIndexed = new ArrayList<Object>(visitor._typeOfValueIndexed);
		String storagePath = null;
		if (SystemParameters.getBoolean(getConf(), "DIP_DISTRIBUTED"))
			storagePath = SystemParameters.getString(getConf(), "STORAGE_CLUSTER_DIR");
		else
			storagePath = SystemParameters.getString(getConf(), "STORAGE_LOCAL_DIR");

		// TODO We assume that there is only one index !!
		/*
		if (_typeOfValueIndexed.get(0) instanceof Integer) {
			_firstRelationStorage = new BerkeleyDBStore(Integer.class, storagePath + "/first");
			_secondRelationStorage = new BerkeleyDBStore(Integer.class, storagePath + "/second");
		} else if (_typeOfValueIndexed.get(0) instanceof Double) {
			_firstRelationStorage = new BerkeleyDBStore(Double.class, storagePath + "/first");
			_secondRelationStorage = new BerkeleyDBStore(Double.class, storagePath + "/second");
		} else if (_typeOfValueIndexed.get(0) instanceof Date) {
			_firstRelationStorage = new BerkeleyDBStore(Date.class, storagePath + "/first");
			_secondRelationStorage = new BerkeleyDBStore(Date.class, storagePath + "/second");
		} else if (_typeOfValueIndexed.get(0) instanceof String) {
			_firstRelationStorage = new BerkeleyDBStore(String.class, storagePath + "/first");
			_secondRelationStorage = new BerkeleyDBStore(String.class, storagePath + "/second");
		} else
			throw new RuntimeException("non supported type");
		LOG.info("Storage with Uniform BDB!");			
		 */

		if (_typeOfValueIndexed.get(0) instanceof Integer) {
			_firstRelationStorage = new BerkeleyDBStoreSkewed(Integer.class, storagePath + "/first");
			_secondRelationStorage = new BerkeleyDBStoreSkewed(Integer.class, storagePath + "/second");
		} else if (_typeOfValueIndexed.get(0) instanceof Double) {
			_firstRelationStorage = new BerkeleyDBStoreSkewed(Double.class, storagePath + "/first");
			_secondRelationStorage = new BerkeleyDBStoreSkewed(Double.class, storagePath + "/second");
		} else if (_typeOfValueIndexed.get(0) instanceof Date) {
			_firstRelationStorage = new BerkeleyDBStoreSkewed(Date.class, storagePath + "/first");
			_secondRelationStorage = new BerkeleyDBStoreSkewed(Date.class, storagePath + "/second");
		} else if (_typeOfValueIndexed.get(0) instanceof String) {
			_firstRelationStorage = new BerkeleyDBStoreSkewed(String.class, storagePath + "/first");
			_secondRelationStorage = new BerkeleyDBStoreSkewed(String.class, storagePath + "/second");
		} else
			throw new RuntimeException("non supported type");
		LOG.info("Storage with Skewed BDB!");

		if (_joinPredicate != null)
			_existIndexes = true;
		else
			_existIndexes = false;
	}

	@Override
	public void execute(Tuple stormTupleRcv) {
		if (_firstTime && MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis)) {
			_periodicAggBatch = new PeriodicAggBatchSend(_aggBatchOutputMillis, this);
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
			final String inputTupleHash = stormTupleRcv.getStringByField(StormComponent.HASH);// getString(2);

			if (processFinalAck(tuple, stormTupleRcv))
				return;

			final String inputTupleString = MyUtilities.tupleToString(tuple, getConf());

			processNonLastTuple(inputComponentIndex, inputTupleString, tuple, inputTupleHash,
					stormTupleRcv, true);

		} else {
			final String inputComponentIndex = stormTupleRcv
					.getStringByField(StormComponent.COMP_INDEX); // getString(0);
			final String inputBatch = stormTupleRcv.getStringByField(StormComponent.TUPLE);// getString(1);

			final String[] wholeTuples = inputBatch
					.split(SystemParameters.MANUAL_BATCH_TUPLE_DELIMITER);
			final int batchSize = wholeTuples.length;
			for (int i = 0; i < batchSize; i++) {
				// parsing
				final String currentTuple = wholeTuples[i];
				final String[] parts = currentTuple
						.split(SystemParameters.MANUAL_BATCH_HASH_DELIMITER);

				String inputTupleHash = null;
				String inputTupleString = null;
				if (parts.length == 1)
					// lastAck
					inputTupleString = parts[0];
				else {
					inputTupleHash = parts[0];
					inputTupleString = parts[1];
				}
				final List<String> tuple = MyUtilities.stringToTuple(inputTupleString, getConf());

				// final Ack check
				if (processFinalAck(tuple, stormTupleRcv)) {
					if (i != batchSize - 1)
						throw new RuntimeException(
								"Should not be here. LAST_ACK is not the last tuple!");
					return;
				}
				// processing a tuple
				if (i == batchSize - 1)
					processNonLastTuple(inputComponentIndex, inputTupleString, tuple,
							inputTupleHash, stormTupleRcv, true);
				else
					processNonLastTuple(inputComponentIndex, inputTupleString, tuple,
							inputTupleHash, stormTupleRcv, false);
			}
		}
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

	@Override
	public String getInfoID() {
		final String str = "DestinationStorage " + getID() + " has ID: " + getID();
		return str;
	}

	@Override
	protected InterchangingComponent getInterComp() {
		return _inter;
	}

	@Override
	public long getNumSentTuples() {
		return _numSentTuples;
	}

	@Override
	public PeriodicAggBatchSend getPeriodicAggBatch() {
		return _periodicAggBatch;
	}

	private void insertIntoBDBStorage(BPlusTreeStore affectedStorage, String key,
			String inputTupleString) {

		if (_typeOfValueIndexed.get(0) instanceof Integer)
			affectedStorage.put(Integer.parseInt(key), inputTupleString);
		else if (_typeOfValueIndexed.get(0) instanceof Double)
			affectedStorage.put(Double.parseDouble(key), inputTupleString);
		else if (_typeOfValueIndexed.get(0) instanceof Date)
			try {
				affectedStorage.put(_format.parse(key), inputTupleString);
			} catch (final ParseException e) {
				e.printStackTrace();
			}
		else if (_typeOfValueIndexed.get(0) instanceof String)
			affectedStorage.put(key, inputTupleString);
		else
			throw new RuntimeException("non supported type");
	}

	private void join(Tuple stormTuple, List<String> tuple, boolean isFromFirstEmitter,
			List<String> oppositeStorage, boolean isLastInBatch) {

		if (oppositeStorage == null || oppositeStorage.size() == 0)
			return;

		for (int i = 0; i < oppositeStorage.size(); i++) {
			String oppositeTupleString = oppositeStorage.get(i);

			long lineageTimestamp = 0;
			if (MyUtilities.isCustomTimestampMode(getConf()))
				lineageTimestamp = stormTuple.getLongByField(StormComponent.TIMESTAMP);
			if (MyUtilities.isStoreTimestamp(getConf(), getHierarchyPosition())) {
				// timestamp has to be removed
				final String parts[] = oppositeTupleString.split("\\@");
				final long storedTimestamp = Long.valueOf(parts[0]);
				oppositeTupleString = parts[1];

				// now we set the maximum TS to the tuple
				if (storedTimestamp > lineageTimestamp)
					lineageTimestamp = storedTimestamp;
			}

			final List<String> oppositeTuple = MyUtilities.stringToTuple(oppositeTupleString,
					getComponentConfiguration());
			List<String> firstTuple, secondTuple;
			if (isFromFirstEmitter) {
				firstTuple = tuple;
				secondTuple = oppositeTuple;
			} else {
				firstTuple = oppositeTuple;
				secondTuple = tuple;
			}

			// Check joinCondition
			// if existIndexes == true, the join condition is already checked
			// before
			if (_joinPredicate == null || _existIndexes
					|| _joinPredicate.test(firstTuple, secondTuple)) { // if
				// null,
				// cross
				// product

				// Create the output tuple by omitting the oppositeJoinKeys
				// (ONLY for equi-joins since they are added
				// by the first relation), if any (in case of cartesian product
				// there are none)
				List<String> outputTuple = null;

				// Cartesian product - Outputs all attributes
				outputTuple = MyUtilities.createOutputTuple(firstTuple, secondTuple);

				applyOperatorsAndSend(stormTuple, outputTuple, lineageTimestamp, isLastInBatch);

			}
		}
	}

	protected void performJoin(Tuple stormTupleRcv, List<String> tuple, boolean isFromFirstEmitter,
			String keyValue, BPlusTreeStore oppositeStorage, boolean isLastInBatch) {

		final List<String> tuplesToJoin = selectTupleToJoin(oppositeStorage, isFromFirstEmitter,
				keyValue);

		join(stormTupleRcv, tuple, isFromFirstEmitter, tuplesToJoin, isLastInBatch);
	}

	@Override
	public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
		super.prepare(map, tc, collector);
		createStorage();
	}

	@Override
	protected void printStatistics(int type) {
		if (_statsUtils.isTestMode())
			if (getHierarchyPosition() == StormComponent.FINAL_COMPONENT) {
				// computing variables
				final int size1 = _firstRelationStorage != null ? _firstRelationStorage.size() : 0;
				final int size2 = _secondRelationStorage != null ? _secondRelationStorage.size()
						: 0;
				final int totalSize = size1 + size2;
				final String ts = _dateFormat.format(_cal.getTime());

				// printing
				if (!MyUtilities.isCustomTimestampMode(getConf())) {
					final Runtime runtime = Runtime.getRuntime();
					final long memory = runtime.totalMemory() - runtime.freeMemory();
					if (type == SystemParameters.INITIAL_PRINT)
						LOG.info("," + "INITIAL," + _thisTaskID + "," + " TimeStamp:," + ts
								+ ", FirstStorage:," + size1 + ", SecondStorage:," + size2
								+ ", Total:," + totalSize + ", Memory used: ,"
								+ StatisticsUtilities.bytesToMegabytes(memory) + ","
								+ StatisticsUtilities.bytesToMegabytes(runtime.totalMemory()));
					else if (type == SystemParameters.INPUT_PRINT)
						LOG.info("," + "MEMORY," + _thisTaskID + "," + " TimeStamp:," + ts
								+ ", FirstStorage:," + size1 + ", SecondStorage:," + size2
								+ ", Total:," + totalSize + ", Memory used: ,"
								+ StatisticsUtilities.bytesToMegabytes(memory) + ","
								+ StatisticsUtilities.bytesToMegabytes(runtime.totalMemory()));
					else if (type == SystemParameters.OUTPUT_PRINT)
						LOG.info("," + "RESULT," + _thisTaskID + "," + "TimeStamp:," + ts
								+ ",Sent Tuples," + getNumSentTuples());
					else if (type == SystemParameters.FINAL_PRINT) {
						if (numNegatives > 0)
							LOG.info("WARNINGLAT! Negative latency for " + numNegatives
									+ ", at most " + maxNegative + "ms.");
						LOG.info("," + "MEMORY," + _thisTaskID + "," + " TimeStamp:," + ts
								+ ", FirstStorage:," + size1 + ", SecondStorage:," + size2
								+ ", Total:," + totalSize + ", Memory used: ,"
								+ StatisticsUtilities.bytesToMegabytes(memory) + ","
								+ StatisticsUtilities.bytesToMegabytes(runtime.totalMemory()));
						LOG.info("," + "RESULT," + _thisTaskID + "," + "TimeStamp:," + ts
								+ ",Sent Tuples," + getNumSentTuples());
					}
				} else // only final statistics is printed if we are measuring
						// latency
				if (type == SystemParameters.FINAL_PRINT) {
					final Runtime runtime = Runtime.getRuntime();
					final long memory = runtime.totalMemory() - runtime.freeMemory();
					if (numNegatives > 0)
						LOG.info("WARNINGLAT! Negative latency for " + numNegatives + ", at most "
								+ maxNegative + "ms.");
					LOG.info("," + "MEMORY," + _thisTaskID + "," + " TimeStamp:," + ts
							+ ", FirstStorage:," + size1 + ", SecondStorage:," + size2
							+ ", Total:," + totalSize + ", Memory used: ,"
							+ StatisticsUtilities.bytesToMegabytes(memory) + ","
							+ StatisticsUtilities.bytesToMegabytes(runtime.totalMemory()));
					LOG.info("," + "RESULT," + _thisTaskID + "," + "TimeStamp:," + ts
							+ ",Sent Tuples," + getNumSentTuples());
				}
				if (_firstRelationStorage != null) {
					// if storage initialized
					LOG.info("First Storage: " + _firstRelationStorage.getStatistics()
							+ "\nEnd of First Storage");
					LOG.info("Second Storage: " + _secondRelationStorage.getStatistics()
							+ "\nEnd of First Storage");
				}
			}
		if (type == SystemParameters.FINAL_PRINT) {
			// TODO should be moved to some other place
			_firstRelationStorage.shutdown();
			_secondRelationStorage.shutdown();
		}
	}

	private void processNonLastTuple(String inputComponentIndex, String inputTupleString, //
			List<String> tuple, // these two are the same
			String inputTupleHash, Tuple stormTupleRcv, boolean isLastInBatch) {
		boolean isFromFirstEmitter = false;

		BPlusTreeStore affectedStorage, oppositeStorage;

		if (_firstEmitterIndex.equals(inputComponentIndex)) {
			// R update
			isFromFirstEmitter = true;
			affectedStorage = _firstRelationStorage;
			oppositeStorage = _secondRelationStorage;
		} else if (_secondEmitterIndex.equals(inputComponentIndex)) {
			// S update
			isFromFirstEmitter = false;
			affectedStorage = _secondRelationStorage;
			oppositeStorage = _firstRelationStorage;
		} else
			throw new RuntimeException("InputComponentName " + inputComponentIndex
					+ " doesn't match neither " + _firstEmitterIndex + " nor "
					+ _secondEmitterIndex + ".");

		if (MyUtilities.isStoreTimestamp(getConf(), getHierarchyPosition())) {
			final long incomingTimestamp = stormTupleRcv.getLongByField(StormComponent.TIMESTAMP);
			inputTupleString = incomingTimestamp + SystemParameters.STORE_TIMESTAMP_DELIMITER
					+ inputTupleString;
		}

		// NEW
		final PredicateUpdateIndexesVisitor visitor = new PredicateUpdateIndexesVisitor(
				isFromFirstEmitter, tuple);
		_joinPredicate.accept(visitor);
		final String keyValue = new ArrayList<String>(visitor._valuesToIndex).get(0);
		// add the stormTuple to the specific storage
		insertIntoBDBStorage(affectedStorage, keyValue, inputTupleString);

		performJoin(stormTupleRcv, tuple, isFromFirstEmitter, keyValue, oppositeStorage,
				isLastInBatch);

		if ((_firstRelationStorage.size() + _secondRelationStorage.size())
				% _statsUtils.getDipInputFreqPrint() == 0)
			printStatistics(SystemParameters.INPUT_PRINT);
	}

	private List<String> selectTupleToJoin(BPlusTreeStore oppositeStorage,
			boolean isFromFirstEmitter, String keyValue) {

		// If there is atleast one index (so we have single join conditions with
		// 1 index per condition)
		// Get the row indices in the storage of the opposite relation that
		// satisfy each join condition (equijoin / inequality)
		// Then take the intersection of the returned row indices since each
		// join condition
		// is separated by AND

		int currentOperator = _operatorForIndexes.get(0);
		// Switch inequality operator if the tuple coming is from the other
		// relation
		if (isFromFirstEmitter) {
			final int operator = currentOperator;

			if (operator == ComparisonPredicate.GREATER_OP)
				currentOperator = ComparisonPredicate.LESS_OP;
			else if (operator == ComparisonPredicate.NONGREATER_OP)
				currentOperator = ComparisonPredicate.NONLESS_OP;
			else if (operator == ComparisonPredicate.LESS_OP)
				currentOperator = ComparisonPredicate.GREATER_OP;
			else if (operator == ComparisonPredicate.NONLESS_OP)
				currentOperator = ComparisonPredicate.NONGREATER_OP;
			// then it is an equal or not equal so we dont switch the
			// operator
			else
				currentOperator = operator;
		}

		// TODO We assume that 1) there is only one index, and consequently
		// 2) JoinPredicate is ComparisonPredicate
		final Object tdiff = ((ComparisonPredicate) _joinPredicate).getDiff();
		final int diff = tdiff != null ? (Integer) tdiff : 0;

		// Get the values from the index (check type first)
		if (_typeOfValueIndexed.get(0) instanceof String)
			return oppositeStorage.get(currentOperator, keyValue, diff);
		// Even if valueIndexed is at first time an integer with
		// precomputation a*col +b, it become a double
		else if (_typeOfValueIndexed.get(0) instanceof Double)
			return oppositeStorage.get(currentOperator, Double.parseDouble(keyValue), diff);
		else if (_typeOfValueIndexed.get(0) instanceof Integer)
			return oppositeStorage.get(currentOperator, Integer.parseInt(keyValue), diff);
		else if (_typeOfValueIndexed.get(0) instanceof Date)
			try {
				return oppositeStorage.get(currentOperator, _format.parse(keyValue), diff);
			} catch (final ParseException e) {
				e.printStackTrace();
				return null;
			}
		else
			throw new RuntimeException("non supported type");

	}
}