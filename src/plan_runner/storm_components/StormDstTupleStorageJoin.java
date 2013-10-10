package plan_runner.storm_components;

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
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.Operator;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.predicates.Predicate;
import plan_runner.storage.TupleStorage;
import plan_runner.storm_components.synchronization.TopologyKiller;
import plan_runner.thetajoin.indexes.Index;
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
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StormDstTupleStorageJoin extends BaseRichBolt implements StormJoin, StormComponent {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormDstTupleStorageJoin.class);

	private int _hierarchyPosition = INTERMEDIATE;

	private final StormEmitter _firstEmitter, _secondEmitter;
	private final TupleStorage _firstRelationStorage, _secondRelationStorage;

	private final String _ID;
	private final String _componentIndex; // a unique index in a list of all the
	// components
	// used as a shorter name, to save some network traffic
	// it's of type int, but we use String to save more space
	private final String _firstEmitterIndex, _secondEmitterIndex;

	private long _numSentTuples = 0;
	private final boolean _printOut;

	private final ChainOperator _operatorChain;
	private OutputCollector _collector;
	private final Map _conf;

	// position to test for equality in first and second emitter
	// join params of current storage then other relation interchangably !!
	List<Integer> _joinParams;

	// output has hash formed out of these indexes
	private final List<Integer> _hashIndexes;
	private final List<ValueExpression> _hashExpressions;

	private final Predicate _joinPredicate;
	// private OptimalPartition _partitioning;

	private List<Index> _firstRelationIndexes, _secondRelationIndexes;
	private List<Integer> _operatorForIndexes;
	private List<Object> _typeOfValueIndexed;

	private boolean _existIndexes = false;

	// for No ACK: the total number of tasks of all the parent compoonents
	private int _numRemainingParents;

	// for batch sending
	private final Semaphore _semAgg = new Semaphore(1, true);
	private boolean _firstTime = true;
	private PeriodicAggBatchSend _periodicBatch;
	private final long _batchOutputMillis;

	private int _thisTaskID;

	private Calendar _cal;
	private final DateFormat _dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
	SimpleDateFormat format = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy");

	private final InterchangingComponent _inter = null;

	private final StatisticsUtilities _statsUtils;

	public StormDstTupleStorageJoin(StormEmitter firstEmitter, StormEmitter secondEmitter,
			ComponentProperties cp, List<String> allCompNames, Predicate joinPredicate,
			int hierarchyPosition, TopologyBuilder builder, TopologyKiller killer, Config conf) {

		_conf = conf;
		_firstEmitter = firstEmitter;
		_secondEmitter = secondEmitter;
		_ID = cp.getName();
		_componentIndex = String.valueOf(allCompNames.indexOf(_ID));
		_batchOutputMillis = cp.getBatchOutputMillis();

		_firstEmitterIndex = String.valueOf(allCompNames.indexOf(_firstEmitter.getName()));
		_secondEmitterIndex = String.valueOf(allCompNames.indexOf(_secondEmitter.getName()));

		_statsUtils = new StatisticsUtilities(_conf, LOG);

		final int firstCardinality = SystemParameters
				.getInt(conf, firstEmitter.getName() + "_CARD");
		final int secondCardinality = SystemParameters.getInt(conf, secondEmitter.getName()
				+ "_CARD");

		final int parallelism = SystemParameters.getInt(conf, _ID + "_PAR");

		_operatorChain = cp.getChainOperator();

		_hashIndexes = cp.getHashIndexes();
		_hashExpressions = cp.getHashExpressions();
		_joinPredicate = joinPredicate;

		_hierarchyPosition = hierarchyPosition;

		InputDeclarer currentBolt = builder.setBolt(_ID, this, parallelism);

		final EquiMatrixAssignment _currentMappingAssignment = new EquiMatrixAssignment(
				firstCardinality, secondCardinality, parallelism, -1);
		final String dim = _currentMappingAssignment.getMappingDimensions();
		LOG.info(_ID + " Initial Dimensions is: " + dim);

		currentBolt = MyUtilities.attachEmitterHash(conf, null, currentBolt, firstEmitter,
				secondEmitter);

		if (_hierarchyPosition == FINAL_COMPONENT && (!MyUtilities.isAckEveryTuple(conf)))
			killer.registerComponent(this, parallelism);

		_printOut = cp.getPrintOut();
		if (_printOut && _operatorChain.isBlocking())
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
		if (MyUtilities.isAggBatchOutputMode(_batchOutputMillis))
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
						tupleSend(MyUtilities.stringToTuple(tuple, _conf), null, 0);

					// clearing
					agg.clearStorage();

					_semAgg.release();
				}
			}
	}

	protected void applyOperatorsAndSend(Tuple stormTupleRcv, List<String> tuple) {
		if (MyUtilities.isAggBatchOutputMode(_batchOutputMillis))
			try {
				_semAgg.acquire();
			} catch (final InterruptedException ex) {
			}
		tuple = _operatorChain.process(tuple);
		if (MyUtilities.isAggBatchOutputMode(_batchOutputMillis))
			_semAgg.release();

		if (tuple == null)
			return;
		_numSentTuples++;
		printTuple(tuple);

		// TODO
		if (_statsUtils.isTestMode() && _numSentTuples % _statsUtils.getDipOutputFreqPrint() == 0)
			if (_hierarchyPosition == StormComponent.FINAL_COMPONENT) {
				_cal = Calendar.getInstance();
				LOG.info("," + "RESULT," + _thisTaskID + "," + "TimeStamp:,"
						+ _dateFormat.format(_cal.getTime()) + ",Sent Tuples," + _numSentTuples);
			}

		if (MyUtilities.isSending(_hierarchyPosition, _batchOutputMillis))
			tupleSend(tuple, stormTupleRcv, 0);

		// Print TimeStamp if this is the last component

	}

	// from IRichBolt
	@Override
	public void cleanup() {

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
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if (_hierarchyPosition != FINAL_COMPONENT)
			// stage not the final
			// one
			declarer.declare(new Fields("CompIndex", "Tuple", "Hash"));
		else if (!MyUtilities.isAckEveryTuple(_conf))
			declarer.declareStream(SystemParameters.EOF_STREAM, new Fields(SystemParameters.EOF));
	}

	@Override
	public void execute(Tuple stormTupleRcv) {
		if (_firstTime && MyUtilities.isAggBatchOutputMode(_batchOutputMillis)) {
			_periodicBatch = new PeriodicAggBatchSend(_batchOutputMillis, this);
			_firstTime = false;
		}

		if (receivedDumpSignal(stormTupleRcv)) {
			MyUtilities.dumpSignal(this, stormTupleRcv, _collector);
			return;
		}

		final String inputComponentIndex = stormTupleRcv.getString(0);
		final List<String> tuple = (List<String>) stormTupleRcv.getValue(1);
		final String inputTupleString = MyUtilities.tupleToString(tuple, _conf);
		final String inputTupleHash = stormTupleRcv.getString(2);

		if (MyUtilities.isFinalAck(tuple, _conf)) {
			_numRemainingParents--;
			if (_numRemainingParents == 0)
				// TODO
				if (_statsUtils.isTestMode())
					if (_hierarchyPosition == StormComponent.FINAL_COMPONENT) {
						final Runtime runtime = Runtime.getRuntime();
						final long memory = runtime.totalMemory() - runtime.freeMemory();
						_cal = Calendar.getInstance();
						LOG.info("," + "MEMORY," + _thisTaskID + "," + " TimeStamp:,"
								+ _dateFormat.format(_cal.getTime()) + ", FirstStorage:,"
								+ (_firstRelationStorage.size()) + ", SecondStorage:,"
								+ (_secondRelationStorage.size()) + ", Total:,"
								+ (_firstRelationStorage.size() + _secondRelationStorage.size())
								+ ", Memory used: ," + StatisticsUtilities.bytesToMegabytes(memory)
								+ "," + StatisticsUtilities.bytesToMegabytes(runtime.totalMemory()));
						LOG.info("," + "RESULT," + _thisTaskID + "," + "TimeStamp:,"
								+ _dateFormat.format(_cal.getTime()) + ",Sent Tuples,"
								+ _numSentTuples);
					}

			MyUtilities.processFinalAck(_numRemainingParents, _hierarchyPosition, _conf,
					stormTupleRcv, _collector, _periodicBatch);
			return;
		}

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
		} else if (_secondEmitterIndex.equals(inputComponentIndex)) {
			// S update
			isFromFirstEmitter = false;
			affectedStorage = _secondRelationStorage;
			oppositeStorage = _firstRelationStorage;
			affectedIndexes = _secondRelationIndexes;
			oppositeIndexes = _firstRelationIndexes;
		} else
			throw new RuntimeException("InputComponentName " + inputComponentIndex
					+ " doesn't match neither " + _firstEmitterIndex + " nor "
					+ _secondEmitterIndex + ".");

		// add the stormTuple to the specific storage
		final int row_id = affectedStorage.insert(inputTupleString);

		List<String> valuesToApplyOnIndex = null;

		if (_existIndexes)
			valuesToApplyOnIndex = updateIndexes(stormTupleRcv, affectedIndexes, row_id);

		performJoin(stormTupleRcv, tuple, inputTupleHash, isFromFirstEmitter, oppositeIndexes,
				valuesToApplyOnIndex, oppositeStorage);

		// TODO
		if (_statsUtils.isTestMode())
			if ((_hierarchyPosition == StormComponent.FINAL_COMPONENT)
					&& ((_firstRelationStorage.size() + _secondRelationStorage.size()) % _statsUtils
							.getDipInputFreqPrint()) == 0) {
				final Runtime runtime = Runtime.getRuntime();
				final long memory = runtime.totalMemory() - runtime.freeMemory();
				_cal = Calendar.getInstance();
				LOG.info("," + "MEMORY," + _thisTaskID + ",:" + " TimeStamp:,"
						+ _dateFormat.format(_cal.getTime()) + ", FirstStorage:,"
						+ (_firstRelationStorage.size()) + ", SecondStorage:,"
						+ (_secondRelationStorage.size()) + ", Total:,"
						+ (_firstRelationStorage.size() + _secondRelationStorage.size())
						+ ", Memory used: ," + StatisticsUtilities.bytesToMegabytes(memory) + ","
						+ StatisticsUtilities.bytesToMegabytes(runtime.totalMemory()));
			}

		_collector.ack(stormTupleRcv);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return _conf;
	}

	// from StormEmitter interface
	@Override
	public String[] getEmitterIDs() {
		return new String[] { _ID };
	}

	// from StormComponent interface
	@Override
	public String getID() {
		return _ID;
	}

	@Override
	public String getInfoID() {
		final String str = "DestinationStorage " + _ID + " has ID: " + _ID;
		return str;
	}

	@Override
	public String getName() {
		return _ID;
	}

	private void join(Tuple stormTuple, List<String> tuple, boolean isFromFirstEmitter,
			TupleStorage oppositeStorage) {

		if (oppositeStorage == null || oppositeStorage.size() == 0)
			return;

		for (int i = 0; i < oppositeStorage.size(); i++) {
			final String oppositeTupleString = oppositeStorage.get(i);

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

				applyOperatorsAndSend(stormTuple, outputTuple);

			}
		}

	}

	protected void performJoin(Tuple stormTupleRcv, List<String> tuple, String inputTupleHash,
			boolean isFromFirstEmitter, List<Index> oppositeIndexes,
			List<String> valuesToApplyOnIndex, TupleStorage oppositeStorage) {

		final TupleStorage tuplesToJoin = new TupleStorage();
		selectTupleToJoin(oppositeStorage, oppositeIndexes, isFromFirstEmitter,
				valuesToApplyOnIndex, tuplesToJoin);
		join(stormTupleRcv, tuple, isFromFirstEmitter, tuplesToJoin);
	}

	@Override
	public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
		_collector = collector;
		if (_inter == null)
			_numRemainingParents = MyUtilities.getNumParentTasks(tc, _firstEmitter, _secondEmitter);
		else
			_numRemainingParents = MyUtilities.getNumParentTasks(tc, _inter);

		_thisTaskID = tc.getThisTaskId();
		// TODO
		final Runtime runtime = Runtime.getRuntime();
		final long memory = runtime.totalMemory() - runtime.freeMemory();
		_cal = Calendar.getInstance();
		LOG.info("," + "INITIAL," + _thisTaskID + ",:" + " TimeStamp:,"
				+ _dateFormat.format(_cal.getTime()) + ", FirstStorage:,"
				+ (_firstRelationStorage.size()) + ", SecondStorage:,"
				+ (_secondRelationStorage.size()) + ", Total:,"
				+ (_firstRelationStorage.size() + _secondRelationStorage.size())
				+ ", Memory used: ," + StatisticsUtilities.bytesToMegabytes(memory) + ","
				+ StatisticsUtilities.bytesToMegabytes(runtime.totalMemory()));
	}

	@Override
	public void printContent() {
		if (_printOut)
			if ((_operatorChain != null) && _operatorChain.isBlocking()) {
				final Operator lastOperator = _operatorChain.getLastOperator();
				if (lastOperator instanceof AggregateOperator)
					MyUtilities.printBlockingResult(_ID, (AggregateOperator) lastOperator,
							_hierarchyPosition, _conf, LOG);
				else
					MyUtilities.printBlockingResult(_ID, lastOperator.getNumTuplesProcessed(),
							lastOperator.printContent(), _hierarchyPosition, _conf, LOG);
			}
	}

	@Override
	public void printTuple(List<String> tuple) {
		if (_printOut)
			if ((_operatorChain == null) || !_operatorChain.isBlocking()) {
				final StringBuilder sb = new StringBuilder();
				sb.append("\nComponent ").append(_ID);
				sb.append("\nReceived tuples: ").append(_numSentTuples);
				sb.append(" Tuple: ").append(MyUtilities.tupleToString(tuple, _conf));
				LOG.info(sb.toString());
			}
	}

	@Override
	public void printTupleLatency(long numSentTuples, long timestamp) {
	}

	private boolean receivedDumpSignal(Tuple stormTuple) {
		return stormTuple.getSourceStreamId()
				.equalsIgnoreCase(SystemParameters.DUMP_RESULTS_STREAM);
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
			else if (_typeOfValueIndexed.get(i) instanceof Date)
				try {
					currentRowIds = currentOpposIndex.getValues(currentOperator,
							format.parse(value));
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

	@Override
	public void tupleSend(List<String> tuple, Tuple stormTupleRcv, long timestamp) {
		final Values stormTupleSnd = MyUtilities.createTupleValues(tuple, timestamp,
				_componentIndex, _hashIndexes, _hashExpressions, _conf);
		MyUtilities.sendTuple(stormTupleSnd, stormTupleRcv, _collector, _conf);
	}

	private List<String> updateIndexes(Tuple stormTupleRcv, List<Index> affectedIndexes, int row_id) {

		final String inputComponentIndex = stormTupleRcv.getString(0); // Table name
		final List<String> tuple = (List<String>) stormTupleRcv.getValue(1); // INPUT
		// TUPLE
		// Get a list of tuple attributes and the key value

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
			else if (typesOfValuesToIndex.get(i) instanceof Date)
				try {
					affectedIndexes.get(i).put(row_id, format.parse(valuesToIndex.get(i)));
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
