package plan_runner.storm_components.theta;

import gnu.trove.list.array.TIntArrayList;

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
import plan_runner.conversion.TypeConversion;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.Operator;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.predicates.Predicate;
import plan_runner.storage.TupleStorage;
import plan_runner.storm_components.InterchangingComponent;
import plan_runner.storm_components.StormBoltComponent;
import plan_runner.storm_components.StormComponent;
import plan_runner.storm_components.StormEmitter;
import plan_runner.storm_components.synchronization.TopologyKiller;
import plan_runner.thetajoin.indexes.Index;
import plan_runner.thetajoin.matrix_mapping.ContentSensitiveMatrixAssignment;
import plan_runner.thetajoin.matrix_mapping.EquiMatrixAssignment;
import plan_runner.thetajoin.matrix_mapping.MatrixAssignment;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.PeriodicAggBatchSend;
import plan_runner.utilities.SystemParameters;
import plan_runner.utilities.statistics.StatisticsUtilities;
import plan_runner.visitors.PredicateCreateIndexesVisitor;
import plan_runner.visitors.PredicateUpdateIndexesVisitor;
import backtype.storm.Config;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;

public class StormThetaJoin extends StormBoltComponent {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormThetaJoin.class);
	private final TupleStorage _firstRelationStorage, _secondRelationStorage;
	private final String _firstEmitterIndex, _secondEmitterIndex;
	private long _numSentTuples = 0;
	private final ChainOperator _operatorChain;
	// position to test for equality in first and second emitter
	// join params of current storage then other relation interchangably !!
	List<Integer> _joinParams;
	private final Predicate _joinPredicate;
	// private OptimalPartition _partitioning;
	private List<Index> _firstRelationIndexes, _secondRelationIndexes;
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

	public StormThetaJoin(StormEmitter firstEmitter, StormEmitter secondEmitter,
			ComponentProperties cp, List<String> allCompNames, Predicate joinPredicate, boolean isPartitioner,
			int hierarchyPosition, TopologyBuilder builder, TopologyKiller killer, Config conf,
			InterchangingComponent interComp, boolean isContentSensitive, TypeConversion wrapper) {

		super(cp, allCompNames, hierarchyPosition, isPartitioner, conf);

		_firstEmitterIndex = String.valueOf(allCompNames.indexOf(firstEmitter.getName()));
		_secondEmitterIndex = String.valueOf(allCompNames.indexOf(secondEmitter.getName()));
		_aggBatchOutputMillis = cp.getBatchOutputMillis();
		_statsUtils = new StatisticsUtilities(getConf(), LOG);
		final int parallelism = SystemParameters.getInt(conf, getID() + "_PAR");
		_operatorChain = cp.getChainOperator();
		_joinPredicate = joinPredicate;
		InputDeclarer currentBolt = builder.setBolt(getID(), this, parallelism);
		
		final MatrixAssignment _currentMappingAssignment;
		if((!isContentSensitive) || (hierarchyPosition != StormComponent.FINAL_COMPONENT && hierarchyPosition != StormComponent.NEXT_TO_DUMMY)){
			final int firstCardinality = SystemParameters.getInt(conf, firstEmitter.getName() + "_CARD");
			final int secondCardinality = SystemParameters.getInt(conf, secondEmitter.getName() + "_CARD");
			// regions assignment exist only for the last-level join
			_currentMappingAssignment = new EquiMatrixAssignment(firstCardinality, secondCardinality, parallelism, -1);
		}else{
			_currentMappingAssignment = new ContentSensitiveMatrixAssignment(conf); //TODO
		}
		
		final String dim = _currentMappingAssignment.toString();
		LOG.info(getID() + " Initial Dimensions is: " + dim);
		
		if (interComp == null)
			currentBolt = MyUtilities.thetaAttachEmitterComponents(currentBolt, firstEmitter,
					secondEmitter, allCompNames, _currentMappingAssignment, conf,wrapper);
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
		_firstRelationStorage = new TupleStorage();
		_secondRelationStorage = new TupleStorage();
		if (_joinPredicate != null) {
			createIndexes();
			_existIndexes = true;
		} else
			_existIndexes = false;
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
					// next to last because we measure the latency of the last operator
					timestamp = System.currentTimeMillis();
			// timestamp = System.nanoTime();
			tupleSend(tuple, stormTupleRcv, timestamp);
		}
		if (MyUtilities.isPrintLatency(getHierarchyPosition(), getConf()))
			printTupleLatency(_numSentTuples - 1, lineageTimestamp);
	}

	private void createIndexes() {
		final PredicateCreateIndexesVisitor visitor = new PredicateCreateIndexesVisitor();
		_joinPredicate.accept(visitor);

		_firstRelationIndexes = new ArrayList<Index>(visitor._firstRelationIndexes);
		_secondRelationIndexes = new ArrayList<Index>(visitor._secondRelationIndexes);
		_operatorForIndexes = new ArrayList<Integer>(visitor._operatorForIndexes);
		_typeOfValueIndexed = new ArrayList<Object>(visitor._typeOfValueIndexed);
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

	private void join(Tuple stormTuple, List<String> tuple, boolean isFromFirstEmitter,
			TupleStorage oppositeStorage, boolean isLastInBatch) {

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
				final long storedTimestamp = Long.valueOf(new String(parts[0]));
				oppositeTupleString = new String(parts[1]);
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
			// Check joinCondition if existIndexes == true, the join condition is already checked before
			if (_joinPredicate == null || _existIndexes
					|| _joinPredicate.test(firstTuple, secondTuple)) {
				// if null, cross product
				// Create the output tuple by omitting the oppositeJoinKeys
				// (ONLY for equi-joins since they are added by the first relation), 
				//if any (in case of cartesian product there are none)
				List<String> outputTuple = null;
				// Cartesian product - Outputs all attributes
				outputTuple = MyUtilities.createOutputTuple(firstTuple, secondTuple);
				applyOperatorsAndSend(stormTuple, outputTuple, lineageTimestamp, isLastInBatch);
			}
		}
	}

	protected void performJoin(Tuple stormTupleRcv, List<String> tuple, String inputTupleHash,
			boolean isFromFirstEmitter, List<Index> oppositeIndexes,
			List<String> valuesToApplyOnIndex, TupleStorage oppositeStorage, boolean isLastInBatch) {
		final TupleStorage tuplesToJoin = new TupleStorage();
		selectTupleToJoin(oppositeStorage, oppositeIndexes, isFromFirstEmitter,
				valuesToApplyOnIndex, tuplesToJoin);
		join(stormTupleRcv, tuple, isFromFirstEmitter, tuplesToJoin, isLastInBatch);
	}

	@Override
	protected void printStatistics(int type) {
		if (_statsUtils.isTestMode())
			if (getHierarchyPosition() == StormComponent.FINAL_COMPONENT || getHierarchyPosition() == StormComponent.NEXT_TO_DUMMY) {
				// computing variables
				final int size1 = _firstRelationStorage.size();
				final int size2 = _secondRelationStorage.size();
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
				} else 	// only final statistics is printed if we are measuring
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
			}
	}

	private void processNonLastTuple(String inputComponentIndex, String inputTupleString, //
			List<String> tuple, // these two are the same
			String inputTupleHash, Tuple stormTupleRcv, boolean isLastInBatch) {
		boolean isFromFirstEmitter = false;
		TupleStorage affectedStorage, oppositeStorage;
		List<Index> affectedIndexes, oppositeIndexes;
		if (_firstEmitterIndex.equals(inputComponentIndex)) {
			// R update
			isFromFirstEmitter = true;
			affectedStorage = _firstRelationStorage;
			oppositeStorage = _secondRelationStorage;
			affectedIndexes = _firstRelationIndexes;
			oppositeIndexes = _secondRelationIndexes;
			sendToStatisticsCollector(tuple, 0);
		} else if (_secondEmitterIndex.equals(inputComponentIndex)) {
			// S update
			isFromFirstEmitter = false;
			affectedStorage = _secondRelationStorage;
			oppositeStorage = _firstRelationStorage;
			affectedIndexes = _secondRelationIndexes;
			oppositeIndexes = _firstRelationIndexes;
			sendToStatisticsCollector(tuple, 1);
		} else
			throw new RuntimeException("InputComponentName " + inputComponentIndex
					+ " doesn't match neither " + _firstEmitterIndex + " nor "
					+ _secondEmitterIndex + ".");
		// add the stormTuple to the specific storage
		if (MyUtilities.isStoreTimestamp(getConf(), getHierarchyPosition())) {
			final long incomingTimestamp = stormTupleRcv.getLongByField(StormComponent.TIMESTAMP);
			inputTupleString = incomingTimestamp + SystemParameters.STORE_TIMESTAMP_DELIMITER
					+ inputTupleString;
		}
		final int row_id = affectedStorage.insert(inputTupleString);
		List<String> valuesToApplyOnIndex = null;
		if (_existIndexes)
			valuesToApplyOnIndex = updateIndexes(inputComponentIndex, tuple, affectedIndexes,
					row_id);
		performJoin(stormTupleRcv, tuple, inputTupleHash, isFromFirstEmitter, oppositeIndexes,
				valuesToApplyOnIndex, oppositeStorage, isLastInBatch);
		if ((_firstRelationStorage.size() + _secondRelationStorage.size())
				% _statsUtils.getDipInputFreqPrint() == 0)
			printStatistics(SystemParameters.INPUT_PRINT);
	}

	private void selectTupleToJoin(TupleStorage oppositeStorage, List<Index> oppositeIndexes,
			boolean isFromFirstEmitter, List<String> valuesToApplyOnIndex, TupleStorage tuplesToJoin) {
		if (!_existIndexes) {
			tuplesToJoin.copy(oppositeStorage);
			return;
		}
		final TIntArrayList rowIds = new TIntArrayList();
		// If there is atleast one index (so we have single join conditions with
		// 1 index per condition)
		// Get the row indices in the storage of the opposite relation that
		// satisfy each join condition (equijoin / inequality)
		// Then take the intersection of the returned row indices since each
		// join condition
		// is separated by AND

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
				currentRowIds = currentOpposIndex.getValues(currentOperator, value);
			// Even if valueIndexed is at first time an integer with
			// precomputation a*col +b, it become a double
			else if (_typeOfValueIndexed.get(i) instanceof Double)
				currentRowIds = currentOpposIndex.getValues(currentOperator,
						Double.parseDouble(value));
			else if (_typeOfValueIndexed.get(i) instanceof Integer)
				currentRowIds = currentOpposIndex.getValues(currentOperator,
						Integer.parseInt(value));
			else if (_typeOfValueIndexed.get(i) instanceof Long)
				currentRowIds = currentOpposIndex.getValues(currentOperator,
						Long.parseLong(value));
			else if (_typeOfValueIndexed.get(i) instanceof Date)
				try {
					currentRowIds = currentOpposIndex.getValues(currentOperator,
							_format.parse(value));
				} catch (final ParseException e) {
					e.printStackTrace();
				}
			else
				throw new RuntimeException("non supported type");
			// Compute the intersection
			// TODO: Search only within the ids that are in rowIds from previous conditions If 
			//nothing returned (and since we want intersection), no need to proceed.
			if (currentRowIds == null)
				return;
			// If it's the first index, add everything. Else keep the
			// intersection
			if (i == 0)
				rowIds.addAll(currentRowIds);
			else
				rowIds.retainAll(currentRowIds);
			// If empty after intersection, return
			if (rowIds.isEmpty())
				return;
		}
		// generate tuplestorage
		for (int i = 0; i < rowIds.size(); i++) {
			final int id = rowIds.get(i);
			tuplesToJoin.insert(oppositeStorage.get(id));
		}
	}

	private List<String> updateIndexes(String inputComponentIndex, List<String> tuple,
			List<Index> affectedIndexes, int row_id) {
		boolean comeFromFirstEmitter;

		if (inputComponentIndex.equals(_firstEmitterIndex))
			comeFromFirstEmitter = true;
		else
			comeFromFirstEmitter = false;
		final PredicateUpdateIndexesVisitor visitor = new PredicateUpdateIndexesVisitor(
				comeFromFirstEmitter, tuple);
		_joinPredicate.accept(visitor);

		final List<String> valuesToIndex = new ArrayList<String>(visitor._valuesToIndex);
		final List<Object> typesOfValuesToIndex = new ArrayList<Object>(
				visitor._typesOfValuesToIndex);

		for (int i = 0; i < affectedIndexes.size(); i++)
			if (typesOfValuesToIndex.get(i) instanceof Integer)
				affectedIndexes.get(i).put(row_id, Integer.parseInt(valuesToIndex.get(i)));
			else if (typesOfValuesToIndex.get(i) instanceof Double)
				affectedIndexes.get(i).put(row_id, Double.parseDouble(valuesToIndex.get(i)));
			else if (typesOfValuesToIndex.get(i) instanceof Long)
				affectedIndexes.get(i).put(row_id, Long.parseLong(valuesToIndex.get(i)));
			else if (typesOfValuesToIndex.get(i) instanceof Date)
				try {
					affectedIndexes.get(i).put(row_id, _format.parse(valuesToIndex.get(i)));
				} catch (final ParseException e) {
					throw new RuntimeException("Parsing problem in StormThetaJoin.updatedIndexes "
							+ e.getMessage());
				}
			else if (typesOfValuesToIndex.get(i) instanceof String)
				affectedIndexes.get(i).put(row_id, valuesToIndex.get(i));
			else
				throw new RuntimeException("non supported type");
		return valuesToIndex;
	}
}