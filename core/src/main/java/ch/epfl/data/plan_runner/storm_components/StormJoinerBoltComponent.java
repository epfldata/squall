package ch.epfl.data.plan_runner.storm_components;

import gnu.trove.list.array.TIntArrayList;

import java.io.UnsupportedEncodingException;
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

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import ch.epfl.data.plan_runner.components.ComponentProperties;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.operators.Operator;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.predicates.Predicate;
import ch.epfl.data.plan_runner.storage.BPlusTreeStorage;
import ch.epfl.data.plan_runner.storage.BerkeleyDBStore;
import ch.epfl.data.plan_runner.storage.BerkeleyDBStoreSkewed;
import ch.epfl.data.plan_runner.storage.TupleStorage;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.thetajoin.indexes.Index;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.PeriodicAggBatchSend;
import ch.epfl.data.plan_runner.utilities.SystemParameters;
import ch.epfl.data.plan_runner.utilities.statistics.StatisticsUtilities;
import ch.epfl.data.plan_runner.visitors.PredicateCreateIndexesVisitor;
import ch.epfl.data.plan_runner.visitors.PredicateUpdateIndexesVisitor;
import ch.epfl.data.plan_runner.window_semantics.WindowSemanticsManager;

public abstract class StormJoinerBoltComponent extends StormBoltComponent {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected String _firstEmitterIndex, _secondEmitterIndex;
	protected ChainOperator _operatorChain;
	protected Predicate _joinPredicate;
	protected long _numSentTuples = 0;

	// join condition
	protected List<Index> _firstRelationIndexes, _secondRelationIndexes;
	protected List<Integer> _operatorForIndexes;
	protected List<Object> _typeOfValueIndexed;
	protected boolean _existIndexes = false;

	// for batch sending
	protected Semaphore _semAgg = new Semaphore(1, true);
	protected boolean _firstTime = true;
	protected PeriodicAggBatchSend _periodicAggBatch;
	protected long _aggBatchOutputMillis;

	// for printing statistics for creating graphs
	protected Calendar _cal = Calendar.getInstance();
	protected DateFormat _statDateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
	protected DateFormat _convDateFormat = new SimpleDateFormat(
			"EEE MMM d HH:mm:ss zzz yyyy");
	protected StatisticsUtilities _statsUtils;
	protected InterchangingComponent _inter = null;

	public StormJoinerBoltComponent(StormEmitter firstEmitter,
			StormEmitter secondEmitter, ComponentProperties cp,
			List<String> allCompNames, Predicate joinPredicate,
			int hierarchyPosition, TopologyBuilder builder,
			TopologyKiller killer, boolean _isEWHPartitioner, Config conf) {
		super(cp, allCompNames, hierarchyPosition, _isEWHPartitioner, conf);
		initialize(firstEmitter, secondEmitter, cp, allCompNames,
				joinPredicate, hierarchyPosition, builder, killer, conf);

	}

	public StormJoinerBoltComponent(StormEmitter firstEmitter,
			StormEmitter secondEmitter, ComponentProperties cp,
			List<String> allCompNames, Predicate joinPredicate,
			int hierarchyPosition, TopologyBuilder builder,
			TopologyKiller killer, Config conf) {
		super(cp, allCompNames, hierarchyPosition, conf);
		initialize(firstEmitter, secondEmitter, cp, allCompNames,
				joinPredicate, hierarchyPosition, builder, killer, conf);
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
						tupleSend(MyUtilities.stringToTuple(tuple, getConf()),
								null, 0);
					// clearing
					agg.clearStorage();
					_semAgg.release();
				}
			}
	}

	protected void applyOperatorsAndSend(Tuple stormTupleRcv,
			List<String> tuple, long lineageTimestamp, boolean isLastInBatch) {
		// long timestamp = 0;
		// if (MyUtilities.isWindowTimestampMode(getConf()))
		// if (getHierarchyPosition() == StormComponent.NEXT_TO_LAST_COMPONENT)
		// A tuple has a non-null timestamp only if the component is
		// next to last
		// because we measure the latency of the last operator
		// timestamp = System.currentTimeMillis();
		if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
			try {
				_semAgg.acquire();
			} catch (final InterruptedException ex) {
			}
		tuple = _operatorChain.process(tuple, lineageTimestamp);
		if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
			_semAgg.release();
		if (tuple == null)
			return;
		_numSentTuples++;
		printTuple(tuple);
		if (_numSentTuples % _statsUtils.getDipOutputFreqPrint() == 0)
			printStatistics(SystemParameters.OUTPUT_PRINT);
		/*
		 * Measuring latency from data sources and taking into account only the
		 * last tuple in the batch
		 * 
		 * if (MyUtilities.isSending(getHierarchyPosition(),
		 * _aggBatchOutputMillis)) { long timestamp = 0; if
		 * (MyUtilities.isCustomTimestampMode(getConf())) timestamp =
		 * stormTupleRcv.getLongByField(StormComponent.TIMESTAMP);
		 * tupleSend(tuple, stormTupleRcv, timestamp); } if
		 * (MyUtilities.isPrintLatency(getHierarchyPosition(), getConf())) {
		 * long timestamp; if (MyUtilities.isManualBatchingMode(getConf())) { if
		 * (isLastInBatch) { timestamp =
		 * stormTupleRcv.getLongByField(StormComponent.TIMESTAMP);
		 * printTupleLatency(_numSentTuples - 1, timestamp); } } else {
		 * timestamp = stormTupleRcv.getLongByField(StormComponent.TIMESTAMP);
		 * printTupleLatency(_numSentTuples - 1, timestamp); } }
		 */
		// Measuring latency from the previous component, and taking into
		// account all the tuples in the batch
		if (MyUtilities
				.isSending(getHierarchyPosition(), _aggBatchOutputMillis)) {

			// TODO Window Semantics
			if (!WindowSemanticsManager.sendTupleIfSlidingWindowSemantics(this,
					tuple, stormTupleRcv, lineageTimestamp))
				tupleSend(tuple, stormTupleRcv, lineageTimestamp);
		}
		if (MyUtilities.isPrintLatency(getHierarchyPosition(), getConf()))
			printTupleLatency(_numSentTuples - 1, lineageTimestamp);
	}

	protected void createIndexes() {
		final PredicateCreateIndexesVisitor visitor = new PredicateCreateIndexesVisitor();
		_joinPredicate.accept(visitor);
		_firstRelationIndexes = new ArrayList<Index>(
				visitor._firstRelationIndexes);
		_secondRelationIndexes = new ArrayList<Index>(
				visitor._secondRelationIndexes);
		_operatorForIndexes = new ArrayList<Integer>(
				visitor._operatorForIndexes);
		_typeOfValueIndexed = new ArrayList<Object>(visitor._typeOfValueIndexed);
	}

	// Specific to Bplustree
	protected void createStorage(BPlusTreeStorage _firstRelationStorage,
			BPlusTreeStorage _secondRelationStorage, Logger LOG) {
		final PredicateCreateIndexesVisitor visitor = new PredicateCreateIndexesVisitor();
		_joinPredicate.accept(visitor);
		_operatorForIndexes = new ArrayList<Integer>(
				visitor._operatorForIndexes);
		_typeOfValueIndexed = new ArrayList<Object>(visitor._typeOfValueIndexed);
		String storagePath = null;
		if (SystemParameters.getBoolean(getConf(), "DIP_DISTRIBUTED"))
			storagePath = SystemParameters.getString(getConf(),
					"STORAGE_CLUSTER_DIR");
		else
			storagePath = SystemParameters.getString(getConf(),
					"STORAGE_LOCAL_DIR");
		// TODO Window semantics
		boolean isWindow;
		if (_windowSize > 0 || _tumblingWindowSize > 0)
			isWindow = true;
		else
			isWindow = false;
		if (MyUtilities.isBDBUniform(getConf())) {
			if (_typeOfValueIndexed.get(0) instanceof Integer) {
				_firstRelationStorage = new BerkeleyDBStore(Integer.class,
						storagePath + "/first/" + this.getName() + _thisTaskID,
						isWindow, _thisTaskID);
				_secondRelationStorage = new BerkeleyDBStore(
						Integer.class,
						storagePath + "/second/" + this.getName() + _thisTaskID,
						isWindow, _thisTaskID);
			} else if (_typeOfValueIndexed.get(0) instanceof Double) {
				_firstRelationStorage = new BerkeleyDBStore(Double.class,
						storagePath + "/first/" + this.getName() + _thisTaskID,
						isWindow, _thisTaskID);
				_secondRelationStorage = new BerkeleyDBStore(
						Double.class,
						storagePath + "/second/" + this.getName() + _thisTaskID,
						isWindow, _thisTaskID);
			} else if (_typeOfValueIndexed.get(0) instanceof Date) {
				_firstRelationStorage = new BerkeleyDBStore(Date.class,
						storagePath + "/first/" + this.getName() + _thisTaskID,
						isWindow, _thisTaskID);
				_secondRelationStorage = new BerkeleyDBStore(
						Date.class,
						storagePath + "/second/" + this.getName() + _thisTaskID,
						isWindow, _thisTaskID);
			} else if (_typeOfValueIndexed.get(0) instanceof String) {
				_firstRelationStorage = new BerkeleyDBStore(String.class,
						storagePath + "/first/" + this.getName() + _thisTaskID,
						isWindow, _thisTaskID);
				_secondRelationStorage = new BerkeleyDBStore(
						String.class,
						storagePath + "/second/" + this.getName() + _thisTaskID,
						isWindow, _thisTaskID);
			} else
				throw new RuntimeException("non supported type");
			LOG.info("Storage with Uniform BDB!");
		} else if (MyUtilities.isBDBSkewed(getConf())) {
			if (_typeOfValueIndexed.get(0) instanceof Integer) {
				_firstRelationStorage = new BerkeleyDBStoreSkewed(
						Integer.class, storagePath + "/first", getConf());
				_secondRelationStorage = new BerkeleyDBStoreSkewed(
						Integer.class, storagePath + "/second", getConf());
			} else if (_typeOfValueIndexed.get(0) instanceof Double) {
				_firstRelationStorage = new BerkeleyDBStoreSkewed(Double.class,
						storagePath + "/first", getConf());
				_secondRelationStorage = new BerkeleyDBStoreSkewed(
						Double.class, storagePath + "/second", getConf());
			} else if (_typeOfValueIndexed.get(0) instanceof Date) {
				_firstRelationStorage = new BerkeleyDBStoreSkewed(Date.class,
						storagePath + "/first", getConf());
				_secondRelationStorage = new BerkeleyDBStoreSkewed(Date.class,
						storagePath + "/second", getConf());
			} else if (_typeOfValueIndexed.get(0) instanceof String) {
				_firstRelationStorage = new BerkeleyDBStoreSkewed(String.class,
						storagePath + "/first", getConf());
				_secondRelationStorage = new BerkeleyDBStoreSkewed(
						String.class, storagePath + "/second", getConf());
			} else
				throw new RuntimeException("non supported type");
			LOG.info("Storage with Skewed BDB!");
		} else {
			throw new RuntimeException("Unsupported BDB type!");
		}
		if (_joinPredicate != null)
			_existIndexes = true;
		else
			_existIndexes = false;
	}

	@Override
	public ChainOperator getChainOperator() {
		return _operatorChain;
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return getConf();
	}

	@Override
	public String getInfoID() {
		final String str = "DestinationStorage " + getID() + " has ID: "
				+ getID();
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

	private void initialize(StormEmitter firstEmitter,
			StormEmitter secondEmitter, ComponentProperties cp,
			List<String> allCompNames, Predicate joinPredicate,
			int hierarchyPosition, TopologyBuilder builder,
			TopologyKiller killer, Config conf) {
		_firstEmitterIndex = String.valueOf(allCompNames.indexOf(firstEmitter
				.getName()));
		_secondEmitterIndex = String.valueOf(allCompNames.indexOf(secondEmitter
				.getName()));
		_operatorChain = cp.getChainOperator();
		_aggBatchOutputMillis = cp.getBatchOutputMillis();
		_joinPredicate = joinPredicate;

	}

	// Specific to BplusTree
	protected void insertIntoBDBStorage(BPlusTreeStorage affectedStorage,
			String key, String inputTupleString) {
		if (_typeOfValueIndexed.get(0) instanceof Integer)
			affectedStorage.put(Integer.parseInt(key), inputTupleString);
		else if (_typeOfValueIndexed.get(0) instanceof Double)
			affectedStorage.put(Double.parseDouble(key), inputTupleString);
		else if (_typeOfValueIndexed.get(0) instanceof Date)
			try {
				affectedStorage.put(_convDateFormat.parse(key),
						inputTupleString);
			} catch (final ParseException e) {
				e.printStackTrace();
			}
		else if (_typeOfValueIndexed.get(0) instanceof String)
			affectedStorage.put(key, inputTupleString);
		else
			throw new RuntimeException("non supported type");
	}

	// Generic: TupleStorage or BplusTree
	protected void join(Tuple stormTuple, List<String> tuple,
			boolean isFromFirstEmitter, List<String> oppositeStorage,
			boolean isLastInBatch) {

		if (oppositeStorage == null || oppositeStorage.size() == 0)
			return;
		for (int i = 0; i < oppositeStorage.size(); i++) {
			// TODO window semantics
			StringBuilder oppositeTupleString = new StringBuilder(
					oppositeStorage.get(i));
			long lineageTimestamp = WindowSemanticsManager
					.joinPreProcessingIfSlidingWindowSemantics(this,
							oppositeTupleString, stormTuple);
			if (lineageTimestamp < 0)
				continue;
			// end TODO
			final List<String> oppositeTuple = MyUtilities
					.stringToTuple(oppositeTupleString.toString(),
							getComponentConfiguration());
			List<String> firstTuple, secondTuple;
			if (isFromFirstEmitter) {
				firstTuple = tuple;
				secondTuple = oppositeTuple;
			} else {
				firstTuple = oppositeTuple;
				secondTuple = tuple;
			}

			// Check joinCondition if existIndexes == true, the join condition
			// is already checked before
			if (_joinPredicate == null || _existIndexes
					|| _joinPredicate.test(firstTuple, secondTuple)) {
				// if null, cross product
				// Create the output tuple by omitting the oppositeJoinKeys
				// (ONLY for equi-joins since they are added by the first
				// relation),
				// if any (in case of cartesian product there are none)
				List<String> outputTuple = null;
				// Cartesian product - Outputs all attributes
				outputTuple = MyUtilities.createOutputTuple(firstTuple,
						secondTuple);
				applyOperatorsAndSend(stormTuple, outputTuple,
						lineageTimestamp, isLastInBatch);
			}
		}
	}

	// Specific for BplusTrees
	protected void performJoin(Tuple stormTupleRcv, List<String> tuple,
			boolean isFromFirstEmitter, String keyValue,
			BPlusTreeStorage oppositeStorage, boolean isLastInBatch) {
		final List<String> tuplesToJoin = selectTupleToJoin(oppositeStorage,
				isFromFirstEmitter, keyValue);
		join(stormTupleRcv, tuple, isFromFirstEmitter, tuplesToJoin,
				isLastInBatch);
	}

	// Specific for TupleStorage
	protected void performJoin(Tuple stormTupleRcv, List<String> tuple,
			String inputTupleHash, boolean isFromFirstEmitter,
			List<Index> oppositeIndexes, List<String> valuesToApplyOnIndex,
			TupleStorage oppositeStorage, boolean isLastInBatch) {
		final List<String> tuplesToJoin = selectTupleToJoin(oppositeStorage,
				oppositeIndexes, isFromFirstEmitter, valuesToApplyOnIndex);
		join(stormTupleRcv, tuple, isFromFirstEmitter, tuplesToJoin,
				isLastInBatch);
	}

	protected void printStatistics(int type, int size1, int size2, Logger LOG) {
		if (_statsUtils.isTestMode())
			if (getHierarchyPosition() == StormComponent.FINAL_COMPONENT
					|| getHierarchyPosition() == StormComponent.NEXT_TO_DUMMY) {
				// computing variables
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

	// Specific for BplusTrees
	protected void processNonLastTuple(String inputComponentIndex,
			List<String> tuple, String inputTupleHash, Tuple stormTupleRcv,
			boolean isLastInBatch, BPlusTreeStorage firstRelationStorage,
			BPlusTreeStorage secondRelationStorage) {
		boolean isFromFirstEmitter = false;
		BPlusTreeStorage affectedStorage, oppositeStorage;
		if (_firstEmitterIndex.equals(inputComponentIndex)) {
			// R update
			isFromFirstEmitter = true;
			affectedStorage = firstRelationStorage;
			oppositeStorage = secondRelationStorage;
		} else if (_secondEmitterIndex.equals(inputComponentIndex)) {
			// S update
			isFromFirstEmitter = false;
			affectedStorage = secondRelationStorage;
			oppositeStorage = firstRelationStorage;
		} else
			throw new RuntimeException("InputComponentName "
					+ inputComponentIndex + " doesn't match neither "
					+ _firstEmitterIndex + " nor " + _secondEmitterIndex + ".");
		// first obtain key
		final PredicateUpdateIndexesVisitor visitor = new PredicateUpdateIndexesVisitor(
				isFromFirstEmitter, tuple);
		_joinPredicate.accept(visitor);
		final String keyValue = new ArrayList<String>(visitor._valuesToIndex)
				.get(0);
		// second, obtain value
		String inputTupleString = MyUtilities.tupleToString(tuple, getConf());
		// TODO window semantics
		// add the stormTuple to the specific storage
		inputTupleString = WindowSemanticsManager
				.AddTimeStampToStoredDataIfWindowSemantics(this,
						inputTupleString, stormTupleRcv);
		// add the stormTuple to the specific storage
		insertIntoBDBStorage(affectedStorage, keyValue, inputTupleString);
		performJoin(stormTupleRcv, tuple, isFromFirstEmitter, keyValue,
				oppositeStorage, isLastInBatch);
		if ((firstRelationStorage.size() + secondRelationStorage.size())
				% _statsUtils.getDipInputFreqPrint() == 0) {
			printStatistics(SystemParameters.INPUT_PRINT);
		}
	}

	// Specific for TupleStorage
	protected void processNonLastTuple(String inputComponentIndex,
			List<String> tuple, String inputTupleHash, Tuple stormTupleRcv,
			boolean isLastInBatch, TupleStorage _firstRelationStorage,
			TupleStorage _secondRelationStorage) {
		boolean isFromFirstEmitter = false;
		String inputTupleString = MyUtilities.tupleToString(tuple, getConf());
		TupleStorage affectedStorage, oppositeStorage;
		List<Index> affectedIndexes, oppositeIndexes;
		if (_firstEmitterIndex.equals(inputComponentIndex)) {
			// R update
			isFromFirstEmitter = true;
			affectedStorage = _firstRelationStorage;
			oppositeStorage = _secondRelationStorage;
			affectedIndexes = _firstRelationIndexes;
			oppositeIndexes = _secondRelationIndexes;
		} else if (_secondEmitterIndex.equals(inputComponentIndex)) {
			// S update
			isFromFirstEmitter = false;
			affectedStorage = _secondRelationStorage;
			oppositeStorage = _firstRelationStorage;
			affectedIndexes = _secondRelationIndexes;
			oppositeIndexes = _firstRelationIndexes;
		} else
			throw new RuntimeException("InputComponentName "
					+ inputComponentIndex + " doesn't match neither "
					+ _firstEmitterIndex + " nor " + _secondEmitterIndex + ".");
		// TODO window semantics
		// add the stormTuple to the specific storage
		inputTupleString = WindowSemanticsManager
				.AddTimeStampToStoredDataIfWindowSemantics(this,
						inputTupleString, stormTupleRcv);
		final int row_id = affectedStorage.insert(inputTupleString);
		List<String> valuesToApplyOnIndex = null;
		if (_existIndexes)
			valuesToApplyOnIndex = updateIndexes(inputComponentIndex, tuple,
					affectedIndexes, row_id);
		performJoin(stormTupleRcv, tuple, inputTupleHash, isFromFirstEmitter,
				oppositeIndexes, valuesToApplyOnIndex, oppositeStorage,
				isLastInBatch);
		if ((_firstRelationStorage.size() + _secondRelationStorage.size())
				% _statsUtils.getDipInputFreqPrint() == 0) {
			printStatistics(SystemParameters.INPUT_PRINT);
		}
	}

	// Specific to Bplustree
	protected List<String> selectTupleToJoin(BPlusTreeStorage oppositeStorage,
			boolean isFromFirstEmitter, String keyValue) {

		// If there is atleast one index (so we have single join conditions with
		// 1 index per condition)
		// Get the row indices in the storage of the opposite relation that
		// satisfy each join condition (equijoin / inequality)
		// Then take the intersection of the returned row indices since each
		// join condition
		// is separated by AND

		final int currentOperator = _operatorForIndexes.get(0);
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
			return oppositeStorage.get(currentOperator,
					Double.parseDouble(keyValue), diff);
		else if (_typeOfValueIndexed.get(0) instanceof Integer)
			return oppositeStorage.get(currentOperator,
					Integer.parseInt(keyValue), diff);
		else if (_typeOfValueIndexed.get(0) instanceof Date)
			try {
				return oppositeStorage.get(currentOperator,
						_convDateFormat.parse(keyValue), diff);
			} catch (final ParseException e) {
				e.printStackTrace();
				return null;
			}
		else
			throw new RuntimeException("non supported type");
	}

	// Specific to TupleStorage
	protected List<String> selectTupleToJoin(TupleStorage oppositeStorage,
			List<Index> oppositeIndexes, boolean isFromFirstEmitter,
			List<String> valuesToApplyOnIndex) {
		List<String> result = new ArrayList<String>();
		if (!_existIndexes) {
			try {
				return oppositeStorage.toList();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		final TIntArrayList rowIds = new TIntArrayList();
		/*
		 * Then take the intersection of the returned row indices of all join
		 * conditions
		 */
		for (int i = 0; i < oppositeIndexes.size(); i++) {
			TIntArrayList currentRowIds = null;
			final Index currentOpposIndex = oppositeIndexes.get(i);
			final String value = valuesToApplyOnIndex.get(i);
			int currentOperator = _operatorForIndexes.get(i);
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
				else
					currentOperator = operator;
			}
			// Get the values from the index (check type first)
			if (_typeOfValueIndexed.get(i) instanceof String)
				currentRowIds = currentOpposIndex.getValues(currentOperator,
						value);
			// Even if valueIndexed is at first time an integer with
			// precomputation a*col +b, it become a double
			else if (_typeOfValueIndexed.get(i) instanceof Double)
				currentRowIds = currentOpposIndex.getValues(currentOperator,
						Double.parseDouble(value));
			else if (_typeOfValueIndexed.get(i) instanceof Integer)
				currentRowIds = currentOpposIndex.getValues(currentOperator,
						Integer.parseInt(value));
			else if (_typeOfValueIndexed.get(i) instanceof Date)
				try {
					currentRowIds = currentOpposIndex.getValues(
							currentOperator, _convDateFormat.parse(value));
				} catch (final ParseException e) {
					e.printStackTrace();
				}
			else
				throw new RuntimeException("non supported type");
			if (currentRowIds == null)
				return result;
			if (i == 0)
				rowIds.addAll(currentRowIds);
			else
				rowIds.retainAll(currentRowIds);
			if (rowIds.isEmpty())
				return result;
		}
		for (int i = 0; i < rowIds.size(); i++) {
			final int id = rowIds.get(i);
			result.add(oppositeStorage.get(id));
		}
		return result;
	}

	// Specific to TupleStorage
	protected List<String> updateIndexes(String inputComponentIndex,
			List<String> tuple, List<Index> affectedIndexes, int row_id) {
		boolean comeFromFirstEmitter = false;
		if (inputComponentIndex.equals(_firstEmitterIndex))
			comeFromFirstEmitter = true;
		final PredicateUpdateIndexesVisitor visitor = new PredicateUpdateIndexesVisitor(
				comeFromFirstEmitter, tuple);
		_joinPredicate.accept(visitor);
		final List<String> valuesToIndex = new ArrayList<String>(
				visitor._valuesToIndex);
		final List<Object> typesOfValuesToIndex = new ArrayList<Object>(
				visitor._typesOfValuesToIndex);
		for (int i = 0; i < affectedIndexes.size(); i++)
			if (typesOfValuesToIndex.get(i) instanceof Integer)
				affectedIndexes.get(i).put(row_id,
						Integer.parseInt(valuesToIndex.get(i)));
			else if (typesOfValuesToIndex.get(i) instanceof Double)
				affectedIndexes.get(i).put(row_id,
						Double.parseDouble(valuesToIndex.get(i)));
			else if (typesOfValuesToIndex.get(i) instanceof Date)
				try {
					affectedIndexes.get(i).put(row_id,
							_convDateFormat.parse(valuesToIndex.get(i)));
				} catch (final ParseException e) {
					throw new RuntimeException(
							"Parsing problem in StormThetaJoin.updatedIndexes "
									+ e.getMessage());
				}
			else if (typesOfValuesToIndex.get(i) instanceof String)
				affectedIndexes.get(i).put(row_id, valuesToIndex.get(i));
			else
				throw new RuntimeException("non supported type");
		return valuesToIndex;
	}

}