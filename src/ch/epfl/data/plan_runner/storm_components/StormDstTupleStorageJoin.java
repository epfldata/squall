package ch.epfl.data.plan_runner.storm_components;

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

import backtype.storm.Config;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import ch.epfl.data.plan_runner.components.ComponentProperties;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.operators.Operator;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.predicates.Predicate;
import ch.epfl.data.plan_runner.storage.TupleStorage;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.thetajoin.indexes.Index;
import ch.epfl.data.plan_runner.thetajoin.matrix_mapping.EquiMatrixAssignment;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.PeriodicAggBatchSend;
import ch.epfl.data.plan_runner.utilities.SystemParameters;
import ch.epfl.data.plan_runner.utilities.statistics.StatisticsUtilities;
import ch.epfl.data.plan_runner.visitors.PredicateCreateIndexesVisitor;
import ch.epfl.data.plan_runner.visitors.PredicateUpdateIndexesVisitor;

public class StormDstTupleStorageJoin extends StormBoltComponent {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger
	    .getLogger(StormDstTupleStorageJoin.class);

    // components
    // used as a shorter name, to save some network traffic
    // it's of type int, but we use String to save more space
    private final String _firstEmitterIndex, _secondEmitterIndex;

    private final TupleStorage _firstRelationStorage, _secondRelationStorage;

    private final ChainOperator _operatorChain;

    private long _numSentTuples = 0;

    // for load-balancing
    private final List<String> _fullHashList;

    // join condition
    private final Predicate _joinPredicate;
    private List<Index> _firstRelationIndexes, _secondRelationIndexes;
    private List<Integer> _operatorForIndexes;
    private List<Object> _typeOfValueIndexed;
    private boolean _existIndexes = false;

    // for batch sending
    private final Semaphore _semAgg = new Semaphore(1, true);
    private boolean _firstTime = true;
    private PeriodicAggBatchSend _periodicAggBatch;
    private final long _aggBatchOutputMillis;

    // for printing statistics for creating graphs
    protected Calendar _cal = Calendar.getInstance();
    protected DateFormat _statDateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    protected DateFormat _convDateFormat = new SimpleDateFormat(
	    "EEE MMM d HH:mm:ss zzz yyyy");
    protected StatisticsUtilities _statsUtils;

    public StormDstTupleStorageJoin(StormEmitter firstEmitter,
	    StormEmitter secondEmitter, ComponentProperties cp,
	    List<String> allCompNames, Predicate joinPredicate,
	    int hierarchyPosition, TopologyBuilder builder,
	    TopologyKiller killer, Config conf) {

	super(cp, allCompNames, hierarchyPosition, conf);

	_firstEmitterIndex = String.valueOf(allCompNames.indexOf(firstEmitter
		.getName()));
	_secondEmitterIndex = String.valueOf(allCompNames.indexOf(secondEmitter
		.getName()));

	_firstRelationStorage = new TupleStorage();
	_secondRelationStorage = new TupleStorage();

	_operatorChain = cp.getChainOperator();
	_fullHashList = cp.getFullHashList();

	_aggBatchOutputMillis = cp.getBatchOutputMillis();

	_statsUtils = new StatisticsUtilities(getConf(), LOG);

	_joinPredicate = joinPredicate;

	final int parallelism = SystemParameters.getInt(getConf(), getID()
		+ "_PAR");
	final int firstCardinality = SystemParameters.getInt(conf,
		firstEmitter.getName() + "_CARD");
	final int secondCardinality = SystemParameters.getInt(conf,
		secondEmitter.getName() + "_CARD");
	final EquiMatrixAssignment currentMappingAssignment = new EquiMatrixAssignment(
		firstCardinality, secondCardinality, parallelism, -1);
	final String dim = currentMappingAssignment.getMappingDimensions();
	LOG.info(getID() + " Initial Dimensions is: " + dim);

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

	/*
	 * Measuring latency from data sources and taking into account only the
	 * last tuple in the batch
	 * 
	 * if (MyUtilities.isSending(getHierarchyPosition(),
	 * _aggBatchOutputMillis)) { long timestamp = 0; if
	 * (MyUtilities.isCustomTimestampMode(getConf())) timestamp =
	 * stormTupleRcv.getLongByField(StormComponent.TIMESTAMP);
	 * tupleSend(tuple, stormTupleRcv, timestamp); }
	 * 
	 * if (MyUtilities.isPrintLatency(getHierarchyPosition(), getConf())) {
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

    // from IRichBolt
    @Override
    public void cleanup() {

    }

    private void createIndexes() {
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

    @Override
    public void execute(Tuple stormTupleRcv) {
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
	getCollector().ack(stormTupleRcv);
    }

    private void processNonLastTuple(String inputComponentIndex,
	    List<String> tuple, String inputTupleHash, Tuple stormTupleRcv,
	    boolean isLastInBatch) {

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

	// add the stormTuple to the specific storage
	if (MyUtilities.isStoreTimestamp(getConf(), getHierarchyPosition())) {
	    final long incomingTimestamp = stormTupleRcv
		    .getLongByField(StormComponent.TIMESTAMP);
	    inputTupleString = incomingTimestamp
		    + SystemParameters.STORE_TIMESTAMP_DELIMITER
		    + inputTupleString;
	}
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

    @Override
    protected void printStatistics(int type) {
	if (_statsUtils.isTestMode())
	    if (getHierarchyPosition() == StormComponent.FINAL_COMPONENT) {
		// computing variables
		final int size1 = _firstRelationStorage.size();
		final int size2 = _secondRelationStorage.size();
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

    // join-specific code
    protected void performJoin(Tuple stormTupleRcv, List<String> tuple,
	    String inputTupleHash, boolean isFromFirstEmitter,
	    List<Index> oppositeIndexes, List<String> valuesToApplyOnIndex,
	    TupleStorage oppositeStorage, boolean isLastInBatch) {

	final TupleStorage tuplesToJoin = new TupleStorage();
	selectTupleToJoin(oppositeStorage, oppositeIndexes, isFromFirstEmitter,
		valuesToApplyOnIndex, tuplesToJoin);
	join(stormTupleRcv, tuple, isFromFirstEmitter, tuplesToJoin,
		isLastInBatch);
    }

    /*
     * TODO: should be put somewhere if (_inter == null) _numRemainingParents =
     * MyUtilities.getNumParentTasks(tc, _firstEmitter, _secondEmitter); else
     * _numRemainingParents = MyUtilities.getNumParentTasks(tc, _inter);
     */

    private void selectTupleToJoin(TupleStorage oppositeStorage,
	    List<Index> oppositeIndexes, boolean isFromFirstEmitter,
	    List<String> valuesToApplyOnIndex, TupleStorage tuplesToJoin) {

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

	    // Compute the intersection
	    // TODO: Search only within the ids that are in rowIds from previous
	    // join conditions
	    // If nothing returned (and since we want intersection), no need to
	    // proceed.
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

    private void join(Tuple stormTuple, List<String> tuple,
	    boolean isFromFirstEmitter, TupleStorage oppositeStorage,
	    boolean isLastInBatch) {

	if (oppositeStorage == null || oppositeStorage.size() == 0)
	    return;

	for (int i = 0; i < oppositeStorage.size(); i++) {
	    String oppositeTupleString = oppositeStorage.get(i);

	    long lineageTimestamp = 0;
	    if (MyUtilities.isCustomTimestampMode(getConf()))
		lineageTimestamp = stormTuple
			.getLongByField(StormComponent.TIMESTAMP);
	    if (MyUtilities.isStoreTimestamp(getConf(), getHierarchyPosition())) {
		// timestamp has to be removed
		final String parts[] = oppositeTupleString.split("\\@");
		final long storedTimestamp = Long.valueOf(new String(parts[0]));
		oppositeTupleString = new String(parts[1]);

		// now we set the maximum TS to the tuple
		if (storedTimestamp > lineageTimestamp)
		    lineageTimestamp = storedTimestamp;
	    }

	    final List<String> oppositeTuple = MyUtilities.stringToTuple(
		    oppositeTupleString, getComponentConfiguration());
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
		outputTuple = MyUtilities.createOutputTuple(firstTuple,
			secondTuple);
		/*
		 * The previous form works when instantiated from Cyclone plans.
		 * The following form is probably required when generated from
		 * Squall.
		 * 
		 * outputTuple = MyUtilities.createOutputTuple(firstTuple,
		 * secondTuple, _rightHashIndexes);
		 */

		applyOperatorsAndSend(stormTuple, outputTuple,
			lineageTimestamp, isLastInBatch);
	    }
	}
    }

    private List<String> updateIndexes(String inputComponentIndex,
	    List<String> tuple, List<Index> affectedIndexes, int row_id) {
	boolean comeFromFirstEmitter;

	if (inputComponentIndex.equals(_firstEmitterIndex))
	    comeFromFirstEmitter = true;
	else
	    comeFromFirstEmitter = false;

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