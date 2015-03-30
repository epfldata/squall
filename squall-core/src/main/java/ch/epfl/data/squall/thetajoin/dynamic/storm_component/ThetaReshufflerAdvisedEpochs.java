/**
 *
 * @author El Seidy
 * This Class is responsible for applying the various mappings i.e. reshuffling the input data
 * and doing some data migrations when necessary.
 */
package ch.epfl.data.squall.thetajoin.dynamic.storm_component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import ch.epfl.data.squall.storm_components.InterchangingComponent;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.StormEmitter;
import ch.epfl.data.squall.thetajoin.dynamic.advisor.Action;
import ch.epfl.data.squall.thetajoin.dynamic.advisor.Advisor;
import ch.epfl.data.squall.thetajoin.dynamic.advisor.Maybe;
import ch.epfl.data.squall.thetajoin.dynamic.advisor.TheoreticalAdvisorNew;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;
import ch.epfl.data.squall.utilities.thetajoin_dynamic.ThetaJoinUtilities;
import ch.epfl.data.squall.utilities.thetajoin_dynamic.ThetaState.state;

public class ThetaReshufflerAdvisedEpochs extends BaseRichBolt {

	/**
	 * 
	 */
	private static Logger LOG = Logger
			.getLogger(ThetaReshufflerAdvisedEpochs.class);
	private static final long serialVersionUID = 1L;
	private OutputCollector _collector;
	private long _currentFirstRelationCount, _currentSecondRelationCount;
	private String _firstEmitterIndex, _secondEmitterIndex;
	private final int _numOfJoinWorkersTasks;
	private boolean _isFinalAckReceived = false;
	private boolean _isFinalAckSent = false;
	private int _numParentTasks;
	private int _numRemainingParentsForLastAck;
	/*
	 * Number of parents (joiners) for Data migration Ended .. a reshuffler
	 * should receive DMEOF signal from all joiners to signal the end of data
	 * migration
	 */
	private int _numRemainingParentsForDMEOF;
	private final Map _conf;
	private state _currentState = state.NORMAL;
	private final StormEmitter _firstEmitter, _secondEmitter;
	private String _ID;
	private String _componentName;
	private final int _reshufflerParallelism;
	private transient InputDeclarer _currentBolt;
	private String _joinerID;
	private List<Integer> _taskPhysicalMapping;
	/*
	 * Index Renamings of joiners, as joiner indexes (TaskIDs) change after
	 * mapping changes
	 */
	private int[] logicalMappings;
	private Action _currentAction = null; /* action to change mapping */
	private final String _initialDim;
	/*
	 * gets a value of N/A if no migration has happened, 1-2 if rel 1 does
	 * Exhchange and rel 2 does discards, and vice versa
	 */
	private String _currentDimExcDis = "N/A";
	private int _taskPhysicalID;
	private int _taskIDLogicalIndex;
	private InterchangingComponent _interComp = null;
	private int _currentEpochNumber = 0;
	private List<Integer> _resufflerIndex;
	public boolean isInterchanging = false;
	private int _currentInterchaningCursor = -1;

	/**
	 * CLASS MEMBERS OF THE ADVISOR-COORDINATOR (DECISION MAKER) assigned to
	 * TASKIDINDEX=0
	 */
	/*
	 * Used for acking from joiners to permit change mapping i.e., you cannot
	 * change mapping while one is being changed
	 */
	private int _currentNumberOfAckedJoinWorkersTasks;
	private Advisor _mAdvisor;
	// private Action _AdvisorcurrentAction = null;
	private state _currentAdvisorState = state.NORMAL;
	private int _hierarchyPosition;

	public ThetaReshufflerAdvisedEpochs(StormEmitter firstRelation,
			StormEmitter secondRelation, List<String> allCompNames,
			int numberOfJoinWorkersTasks, int hierarchyPosition, Map conf,
			TopologyBuilder builder, String initialDim) {
		_firstEmitter = firstRelation;
		_secondEmitter = secondRelation;

		_hierarchyPosition = hierarchyPosition;
		if (_hierarchyPosition == StormComponent.FINAL_COMPONENT)
			_hierarchyPosition = StormComponent.NEXT_TO_LAST_COMPONENT;

		/* If This is an interchanging emitter */
		if (secondRelation == null) {
			_firstEmitterIndex = String
					.valueOf(allCompNames.indexOf(new String(_firstEmitter
							.getName().split("-")[0])));
			_secondEmitterIndex = String
					.valueOf(allCompNames.indexOf(new String(_firstEmitter
							.getName().split("-")[1])));
			_componentName = new String(_firstEmitter.getName().split("-")[0])
					+ "_" + new String(_firstEmitter.getName().split("-")[1]);
			_ID = _componentName + "_RESHUFFLER";

		} else {
			_firstEmitterIndex = String.valueOf(allCompNames
					.indexOf(_firstEmitter.getName()));
			_secondEmitterIndex = String.valueOf(allCompNames
					.indexOf(_secondEmitter.getName()));
			_ID = firstRelation.getName() + "_" + secondRelation.getName()
					+ "_RESHUFFLER";
			_componentName = firstRelation.getName() + "_"
					+ secondRelation.getName();
		}
		_numOfJoinWorkersTasks = numberOfJoinWorkersTasks;
		_numRemainingParentsForDMEOF = numberOfJoinWorkersTasks;
		_conf = conf;
		_initialDim = initialDim;
		_reshufflerParallelism = SystemParameters.getInt(conf, _componentName
				+ "_RESHUF_PAR");
		_currentBolt = builder.setBolt(String.valueOf(_ID), this,
				_reshufflerParallelism);

		preProcess();
		// **************************
	}

	protected void advise() {
		Action advisorcurrentAction = null;
		final Maybe<Action> isAction = _mAdvisor.advise();
		if (!isAction.isNone())// there is no pending Assignment
			if (_currentAdvisorState != state.DATAMIGRATING
					&& _currentAdvisorState != state.FLUSHING) {
				advisorcurrentAction = isAction.get();
				_currentAdvisorState = state.FLUSHING;
				LOG.info(_componentName + ":" + _taskIDLogicalIndex
						+ " MAIN: sending new mapping:"
						+ advisorcurrentAction.toString() + " Relation sizes "
						+ _currentFirstRelationCount + ","
						+ _currentSecondRelationCount);

				for (int i = 1; i < _resufflerIndex.size(); i++)
					_collector.emitDirect(_resufflerIndex.get(i),
							SystemParameters.ThetaSynchronizerSignal,
							new Values(SystemParameters.ThetaSignalStop,
									advisorcurrentAction.toString()));
				// emit the stop signal with mapping
				_mAdvisor.updateMapping(advisorcurrentAction);
				// **************************Local changes
				_currentState = state.FLUSHING;
				_currentEpochNumber++;
				_currentAction = advisorcurrentAction;
				_currentDimExcDis = getNewMapping(_currentAction);

				LOG.info(_ID
						+ ":"
						+ _taskPhysicalID
						+ " :Reshuffler (sending STOP) & current mapping changing to :("
						+ _currentAction.toString() + ") with epoch number:"
						+ _currentEpochNumber + " (" + _mAdvisor.totalRowTuples
						+ "," + _mAdvisor.totalColumnTuples + ")");
				updateLogicalMappings();
				_collector.emit(SystemParameters.ThetaReshufflerSignal,
						new Values(SystemParameters.ThetaSignalStop,
								advisorcurrentAction.toString()));
				// ///*****************************
			}
	}

	private void appendTimestampMigration(Tuple stormTupleRcv, Values tplSend) {
		if (MyUtilities.isCustomTimestampMode(_conf)) {
			final long timestamp = stormTupleRcv
					.getLongByField(StormComponent.TIMESTAMP);
			tplSend.add(timestamp);
		}
	}

	private void appendTimestampNew(Values tplSend) {
		if (MyUtilities.isCustomTimestampMode(_conf)) {
			long timestamp = 0;
			if (_hierarchyPosition == StormComponent.NEXT_TO_LAST_COMPONENT)
				// A tuple has a non-null timestamp only if the component is
				// next to last
				// because we measure the latency of the last operator
				timestamp = System.currentTimeMillis();
			tplSend.add(timestamp);
		}
	}

	/**
	 * The Reshuffler can send: 1) Normal Datastream tuples (from
	 * previouslayers) to the ThetaJoiner 2) Migrated Datastream tuples (from
	 * the ThetaJoinerDynamic) 3) Aggregated Counts to the ThetaMappingAssigner
	 * 4) Signal (stop, proceed(migrate data), DataMigrationEnded) /Mapping to
	 * the ThetaJoinerDynamic
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 0) LAST_ACK and EOF datastreams
		final ArrayList<String> EOFStreamFields = new ArrayList<String>();
		EOFStreamFields.add(StormComponent.COMP_INDEX);
		EOFStreamFields.add(StormComponent.TUPLE);
		EOFStreamFields.add(StormComponent.HASH);
		if (MyUtilities.isCustomTimestampMode(_conf))
			EOFStreamFields.add(StormComponent.TIMESTAMP);
		declarer.declare(new Fields(EOFStreamFields));

		// 1) Theta Datastream tuples
		final ArrayList<String> dataStreamFields = new ArrayList<String>();
		dataStreamFields.add(StormComponent.COMP_INDEX);
		dataStreamFields.add(StormComponent.TUPLE);
		dataStreamFields.add(StormComponent.HASH);
		dataStreamFields.add(StormComponent.EPOCH);
		dataStreamFields.add(StormComponent.DIM);
		if (MyUtilities.isCustomTimestampMode(_conf))
			dataStreamFields.add(StormComponent.TIMESTAMP);
		declarer.declareStream(SystemParameters.ThetaDataReshufflerToJoiner,
				true, new Fields(dataStreamFields)); // direct streams

		// 2) Theta Datastream tuples, migrated data
		final ArrayList<String> thetaStreamFields = new ArrayList<String>();
		thetaStreamFields.add(StormComponent.COMP_INDEX);
		thetaStreamFields.add(StormComponent.TUPLE);
		thetaStreamFields.add(StormComponent.HASH);
		thetaStreamFields.add(StormComponent.EPOCH);
		thetaStreamFields.add(StormComponent.DIM);
		if (MyUtilities.isCustomTimestampMode(_conf))
			thetaStreamFields.add(StormComponent.TIMESTAMP);
		declarer.declareStream(
				SystemParameters.ThetaDataMigrationReshufflerToJoiner, true,
				new Fields(thetaStreamFields)); // direct streams

		// 3) Signal to Joiners
		declarer.declareStream(SystemParameters.ThetaReshufflerSignal,
				new Fields(StormComponent.RESH_SIGNAL, StormComponent.MAPPING));

		// 4)Advisor related
		declarer.declareStream(SystemParameters.ThetaSynchronizerSignal,
				new Fields(StormComponent.RESH_SIGNAL, StormComponent.MAPPING));

	}

	/**
	 * The ThetaReshuffler can receive: 1) ThetaAckDatamigrationEnded to
	 * Controller only 2) Datastream tuples from previous relations (stages). 3)
	 * Datastream tuples from the ThetaJoiner. (migrated data from the next
	 * stage) 4) Signals (Stop or Proceed or DataMigrationEnded) and might
	 * contain new "Mapping" along side from the ThetaMappingAssigner.
	 */
	@Override
	public void execute(Tuple stormTupleRcv) {

		final String inputStream = stormTupleRcv.getSourceStreamId();

		/**
		 * Case 2) Datastream tuples from previous levels
		 */
		if (inputStream.equals(SystemParameters.DATA_STREAM)) {
			final String inputComponentIndex = stormTupleRcv
					.getStringByField(StormComponent.COMP_INDEX);
			final List<String> tupleList = (List<String>) stormTupleRcv
					.getValueByField(StormComponent.TUPLE);
			final String inputTupleString = MyUtilities.tupleToString(
					tupleList, _conf);

			// INPUT TUPLE FINALACK~ end has reached
			if (MyUtilities.isFinalAck(tupleList, _conf)) {
				processFinalAck(stormTupleRcv);
				return;
			}
			final String inputTupleHash = stormTupleRcv
					.getStringByField(StormComponent.HASH);
			// 1)increment the corresponding relation counter.
			// 2)Forward the tuple to the meant region (emit direct)
			int[] currDim = getCurrentMappingDimensions();
			updateStatisticsAndEmit(inputComponentIndex, currDim,
					inputTupleString, inputTupleHash);

			/**
			 * ADVISOR (TASKID=0) UPDATE MAPPING
			 */
			if (_taskIDLogicalIndex == 0 && !_isFinalAckReceived)
				advise();
		}
		/**
		 * Case 1) ADVISOR (only at TASKID=0) ThetaAckDatamigrationEnded
		 */
		else if (inputStream.equals(SystemParameters.ThetaJoinerAcks)) {
			final String AckMessage = stormTupleRcv
					.getStringByField(StormComponent.MESSAGE);
			if (AckMessage.equals(SystemParameters.ThetaAckDataMigrationEnded))
				processDataMigrationAck(stormTupleRcv);
		}

		/**
		 * Case 3) Datastream tuples from the ThetaJoiner. (Migration)
		 */
		else if (inputStream
				.equals(SystemParameters.ThetaDataMigrationJoinerToReshuffler)) {
			final String inputComponentIndex = stormTupleRcv
					.getStringByField(StormComponent.COMP_INDEX);
			final List<String> tupleList = (List<String>) stormTupleRcv
					.getValueByField(StormComponent.TUPLE);
			final String inputTupleString = MyUtilities.tupleToString(
					tupleList, _conf); // INPUT TUPLE
			final String inputTupleHash = stormTupleRcv
					.getStringByField(StormComponent.HASH); // HashTuple

			// If ThetaJoinerMigrationSignal, send a signal back to reshuffler
			// the next batch.
			if (inputTupleString
					.equals(SystemParameters.ThetaJoinerMigrationSignal)) {
				processMigrationSignal(stormTupleRcv);
				return;
			}
			// if its a DMEOF then send signal dataMigrationEnded to all the
			// joiners
			else if (inputTupleString
					.equals(SystemParameters.ThetaJoinerDataMigrationEOF)) {
				processMigrationEOF(stormTupleRcv);
				return;
			}
			// in this case tuples received should have the new epoch number
			// Assertion
			if (!stormTupleRcv.getIntegerByField(StormComponent.EPOCH).equals(
					_currentEpochNumber))
				throw new RuntimeException(
						"Error --> Data migrated tuple has sent an outdated epoch number");
			// 1) Dont increment the counters. (this is only data migration)
			// 2) Forward the tuple to the meant (new) region.
			forwardTuple(inputComponentIndex, stormTupleRcv, inputTupleString,
					inputTupleHash);
		}
		/**
		 * Case 4) Signals (Stop or Proceed or DataMigrationEnded) and might
		 * contain new "Mapping" along side. from the ThetaMappingAssigner.
		 */
		else if (inputStream.equals(SystemParameters.ThetaSynchronizerSignal))
			finalizeMigrationEnded(stormTupleRcv);
		else
			throw new RuntimeException(_ID + ":" + _taskPhysicalID
					+ " ERROR: WRONG STREAM ID!!!");
		_collector.ack(stormTupleRcv);
	}

	protected void finalizeMigrationEnded(Tuple stormTupleRcv) {
		final String signal = stormTupleRcv
				.getStringByField(StormComponent.RESH_SIGNAL);
		// - if Signal is Stop, set the new mapping
		if (signal.equals(SystemParameters.ThetaSignalStop))
			processStopSignal(stormTupleRcv);
		// - if Signal is DataMigration phase has ended
		else if (signal.equals(SystemParameters.ThetaSignalDataMigrationEnded)) {
			LOG.info(_ID + ":" + _taskPhysicalID
					+ " Reshuffler received ThetaSignalDataMigrationEnded");
			_currentState = state.NORMAL;
			// check if isFinalAcked=true --> emit LAST_ACK
			if (_isFinalAckReceived && !_isFinalAckSent) {
				LOG.info(_ID
						+ ":"
						+ _taskPhysicalID
						+ " Reshuffler received ThetaSignalDataMigrationEnded & sent LAST_ACK");
				MyUtilities.processFinalAck(_numRemainingParentsForLastAck,
						StormComponent.INTERMEDIATE, _conf, stormTupleRcv,
						_collector, null);
			}
		}
	}

	/* For renamings */
	private int fooLogicalMappings(int[] renamings2, int k) {
		for (int i = 0; i < renamings2.length; ++i)
			if (renamings2[i] == k)
				return i;
		return -1;
	}

	/*
	 * This method is used for sending the migrated data
	 */
	protected void forwardTuple(String inputComponentIndex,
			Tuple stormTupleRcv, String inputTupleString, String inputTupleHash) {
		final int sourceID = logicalMappings[_taskPhysicalMapping
				.indexOf(stormTupleRcv.getSourceTask())];
		final int[] taskIndices;
		if (_firstEmitterIndex.equals(inputComponentIndex))
			taskIndices = _currentAction
					.getRowExchangeReducersByNewId(sourceID);
		else if (_secondEmitterIndex.equals(inputComponentIndex))
			taskIndices = _currentAction
					.getColumnExchangeReducersByNewId(sourceID);
		else
			throw new RuntimeException(
					"inputComponentIndex does not conform with any relation");
		for (int i = 0; i < taskIndices.length; i++) {
			final Values tplSend = new Values(inputComponentIndex,
					MyUtilities.stringToTuple(inputTupleString, _conf),
					inputTupleHash, _currentEpochNumber, _currentDimExcDis);
			appendTimestampMigration(stormTupleRcv, tplSend);
			_collector.emitDirect(_taskPhysicalMapping.get(fooLogicalMappings(
					logicalMappings, taskIndices[i])),
					SystemParameters.ThetaDataMigrationReshufflerToJoiner,
					tplSend);
		}
	}

	public InterchangingComponent get_interComp() {
		return _interComp;
	}

	public InputDeclarer getCurrentBolt() {
		return _currentBolt;
	}

	protected int[] getCurrentMappingDimensions() {
		if (_currentAction == null)
			return ThetaJoinUtilities.getDimensions(_initialDim);
		else
			return new int[] { _currentAction.getNewRows(),
					_currentAction.getNewColumns() };
	}

	public String getID() {
		return _ID;
	}

	public String getInfoID() {
		final String str = "ThetaReshuffler " + _componentName + "_RESHUF"
				+ " has ID: " + _ID;
		return str;
	}

	private String getNewMapping(Action currentAction) {
		final int exchgDim = identifyDim(_currentAction.getPreviousRows(),
				_currentAction.getPreviousColumns(),
				_currentAction.getNewRows(), _currentAction.getNewColumns(),
				false);
		final int disDim = (exchgDim == 1) ? 2 : 1;
		return exchgDim + "-" + disDim;
	}

	public int getReshufflerParallelism() {
		return _reshufflerParallelism;
	}

	private int identifyDim(int prev1, int prev2, int curr1, int curr2,
			boolean isDiscarding) {
		return ThetaJoinerDynamicAdvisedEpochs.identifyDim(prev1, prev2, curr1,
				curr2, isDiscarding);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_taskPhysicalID = context.getThisTaskId();
		_taskIDLogicalIndex = context.getThisTaskIndex();
		_resufflerIndex = context.getComponentTasks(getID());
		if (_taskIDLogicalIndex != 0)
			_mAdvisor = null;
		_taskPhysicalMapping = context.getComponentTasks(_joinerID);
		_collector = collector;
		if (_secondEmitter == null) // this is an interchanging data source
			_numParentTasks = 1;
		else if (_interComp == null)
			_numParentTasks = MyUtilities.getNumParentTasks(context,
					_firstEmitter, _secondEmitter);
		else {
			isInterchanging = true;
			_numParentTasks = MyUtilities
					.getNumParentTasks(context, _interComp);
		}
		_numRemainingParentsForLastAck = _numParentTasks;
	}

	/*
	 * TODO FOR ALEKSANDAR 1) Renamings is only used for Theta-joins, you dont
	 * need it for M-bucket 2) _mAdvisor needs to be different for you, this is
	 * the "Partitioning scheme and Decision core"
	 */
	protected void preProcess() {
		logicalMappings = new int[_numOfJoinWorkersTasks];
		for (int i = 0; i < logicalMappings.length; i++)
			logicalMappings[i] = i;

		final int[] initialDims = ThetaJoinUtilities.getDimensions(_initialDim);
		_mAdvisor = new TheoreticalAdvisorNew(_numOfJoinWorkersTasks,
				initialDims[0], initialDims[1], _conf);

	}

	protected void processDataMigrationAck(Tuple stormTupleRcv) {
		// 1)increment the _currentNumberOfAckedJoinWorkersTasks
		_currentNumberOfAckedJoinWorkersTasks++;
		// 2)if all acks have been received (equal to the number of
		// ThetaJoinerDynamic tasks) send the
		// ThetaSignalDataMigrationEnded signal & zero the value of
		// _currentNumberOfAckedJoinWorkersTasks
		if (_currentNumberOfAckedJoinWorkersTasks == _numOfJoinWorkersTasks) {
			_currentAdvisorState = state.NORMAL; // change state
			_currentNumberOfAckedJoinWorkersTasks = 0; // reset;
			LOG.info(_componentName
					+ " :synchronizer emitting Datamigration ended"
					+ " with epoch number:" + _currentEpochNumber + " ("
					+ _mAdvisor.totalRowTuples + ","
					+ _mAdvisor.totalColumnTuples + ")");
			for (int i = 1; i < _resufflerIndex.size(); i++)
				_collector.emitDirect(_resufflerIndex.get(i),
						SystemParameters.ThetaSynchronizerSignal, new Values(
								SystemParameters.ThetaSignalDataMigrationEnded,
								"N/A")); // emit the proceed signal
			// **************************Local changes
			_currentState = state.NORMAL;
			// check if isFinalAcked=true --> emit LAST_ACK
			if (_isFinalAckReceived && !_isFinalAckSent) {
				LOG.info(_ID
						+ ":"
						+ _taskPhysicalID
						+ " Reshuffler received ThetaSignalDataMigrationEnded & sent LAST_ACK ("
						+ (_currentFirstRelationCount * _reshufflerParallelism)
						+ ","
						+ (_currentSecondRelationCount * _reshufflerParallelism)
						+ ")");
				MyUtilities.processFinalAck(_numRemainingParentsForLastAck,
						StormComponent.INTERMEDIATE, _conf, stormTupleRcv,
						_collector, null);
			}
		}
	}

	protected void processFinalAck(Tuple stormTupleRcv) {
		_numRemainingParentsForLastAck--;
		LOG.info(_componentName
				+ " :Reshuffler received one LAST_ACK remaining:"
				+ _numRemainingParentsForLastAck);
		if (_numRemainingParentsForLastAck == 0)
			if (_currentState == state.FLUSHING
					|| _currentState == state.DATAMIGRATING)
				_isFinalAckReceived = true;
			else {// final ack has been received but not sent
				_isFinalAckReceived = true;
				_isFinalAckSent = true;
				MyUtilities.processFinalAck(_numRemainingParentsForLastAck,
						StormComponent.INTERMEDIATE, _conf, stormTupleRcv,
						_collector, null);
				if (_taskIDLogicalIndex == 0)
					LOG.info(_componentName
							+ ":"
							+ _taskPhysicalID
							+ " :Reshuffler sending LAST_ACK.....1: ("
							+ (_currentFirstRelationCount * _reshufflerParallelism)
							+ ","
							+ (_currentSecondRelationCount * _reshufflerParallelism)
							+ ")");
			}
		_collector.ack(stormTupleRcv);
	}

	protected void processMigrationEOF(Tuple stormTupleRcv) {
		_numRemainingParentsForDMEOF--;
		// LOG.info(_componentName+":"+_taskID+" :Reshuffler received one DMEOF remaining:"+_numRemainingParentsForDMEOF+" Buffered Storage:"+bufferedTuples.size());
		if (_numRemainingParentsForDMEOF == 0) {
			LOG.info(_componentName
					+ " :Reshuffler emitting received last DEMOF");
			_collector.emit(SystemParameters.ThetaReshufflerSignal, new Values(
					SystemParameters.ThetaSignalDataMigrationEnded, "N/A"));// has
																			// to
																			// be
																			// broadcasted
			_numRemainingParentsForDMEOF = _numOfJoinWorkersTasks; // restart
																	// the
																	// counter
		}
		_collector.ack(stormTupleRcv);

	}

	protected void processMigrationSignal(Tuple stormTupleRcv) {
		final int sentFromTaskId = stormTupleRcv.getSourceTask();
		final Values tplSend = new Values("N/A", MyUtilities.stringToTuple(
				SystemParameters.ThetaJoinerMigrationSignal, _conf), "N/A", -1,
				"N/A");
		appendTimestampMigration(stormTupleRcv, tplSend);
		_collector.emitDirect(sentFromTaskId,
				SystemParameters.ThetaDataMigrationReshufflerToJoiner, tplSend);
		_collector.ack(stormTupleRcv);

	}

	protected void processStopSignal(Tuple stormTupleRcv) {
		final String mapping = stormTupleRcv
				.getStringByField(StormComponent.MAPPING);
		_currentState = state.FLUSHING;
		final String actionString = mapping;
		_currentEpochNumber++;
		_currentAction = Action.fromString(actionString);
		_currentDimExcDis = getNewMapping(_currentAction);
		LOG.info(_ID
				+ ":"
				+ _taskPhysicalID
				+ " :Reshuffler (sending STOP) & current mapping changing to : "
				+ _currentAction.toString() + " with epoch number:"
				+ _currentEpochNumber);
		updateLogicalMappings();
		_collector.emit(SystemParameters.ThetaReshufflerSignal, new Values(
				SystemParameters.ThetaSignalStop, actionString));

	}

	public void set_interComp(InterchangingComponent _interComp) {
		this._interComp = _interComp;
	}

	public void setJoinerID(String _joinerID) {
		this._joinerID = _joinerID;
	}

	private void updateLogicalMappings() {
		final int temp[] = new int[logicalMappings.length];
		for (int i = 0; i < logicalMappings.length; i++)
			temp[i] = _currentAction.getNewReducerName(logicalMappings[i]);
		logicalMappings = temp;
	}

	/*
	 * This method is used for emitting data to a random row or column
	 */
	protected void updateStatisticsAndEmit(String inputComponentIndex,
			int[] currDim, String inputTupleString, String inputTupleHash) {

		long deltaRel1, deltaRel2;
		int interchangingIndex;
		boolean isFirstRel;

		if (_firstEmitterIndex.equals(inputComponentIndex)) {
			_currentFirstRelationCount++;
			interchangingIndex = 1;
			deltaRel1 = _reshufflerParallelism;
			deltaRel2 = 0;
			isFirstRel = true;
		} else if (_secondEmitterIndex.equals(inputComponentIndex)) {
			_currentSecondRelationCount++;
			interchangingIndex = 2;
			deltaRel1 = 0;
			deltaRel2 = _reshufflerParallelism;
			isFirstRel = false;
		} else
			throw new RuntimeException("Not suitable inputComponentIndex");

		if (_taskIDLogicalIndex == 0) {// FOR ADVISOR ONLY
			// ///***********//////////
			if (isInterchanging) {
				if (_currentInterchaningCursor != interchangingIndex) {
					LOG.info(_ID + ":" + _taskPhysicalID
							+ " :Reshuffler INTERCHANGING adding at "
							+ interchangingIndex + " with epoch number:"
							+ _currentEpochNumber + " ("
							+ _mAdvisor.totalRowTuples + ","
							+ _mAdvisor.totalColumnTuples + ")");
					_currentInterchaningCursor = interchangingIndex;
				}
			}
			// ///***********//////////
			_mAdvisor.updateTuples(deltaRel1, deltaRel2);
		}

		final int[] taskIndices = Advisor.getAssignedReducers(isFirstRel,
				_numOfJoinWorkersTasks, currDim[0], currDim[1]);
		for (int i = 0; i < taskIndices.length; i++) {
			final Values tplSend = new Values(inputComponentIndex,
					MyUtilities.stringToTuple(inputTupleString, _conf),
					inputTupleHash, _currentEpochNumber, _currentDimExcDis);
			appendTimestampNew(tplSend);
			_collector.emitDirect(_taskPhysicalMapping.get(fooLogicalMappings(
					logicalMappings, taskIndices[i])),
					SystemParameters.ThetaDataReshufflerToJoiner, tplSend);
		}
	}

}
