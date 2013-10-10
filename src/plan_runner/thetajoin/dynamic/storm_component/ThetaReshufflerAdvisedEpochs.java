/**
 *
 * @author El Seidy
 * This Class is responsible for applying the various mappings i.e. reshuffling the input data
 * and doing some data migrations when necessary.
 */
package plan_runner.thetajoin.dynamic.storm_component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.storm_components.InterchangingComponent;
import plan_runner.storm_components.StormComponent;
import plan_runner.storm_components.StormEmitter;
import plan_runner.thetajoin.dynamic.advisor.Action;
import plan_runner.thetajoin.dynamic.advisor.Advisor;
import plan_runner.thetajoin.dynamic.advisor.Maybe;
import plan_runner.thetajoin.dynamic.advisor.TheoreticalAdvisorNew;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.SystemParameters;
import plan_runner.utilities.thetajoin.dynamic.ThetaJoinUtilities;
import plan_runner.utilities.thetajoin.dynamic.ThetaState.state;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ThetaReshufflerAdvisedEpochs extends BaseRichBolt {

	/**
	 * 
	 */
	private static Logger LOG = Logger.getLogger(ThetaReshufflerAdvisedEpochs.class);
	private static final long serialVersionUID = 1L;
	private OutputCollector _collector;
	private long _currentFirstRelationCount, _currentSecondRelationCount;
	private String _firstEmitterIndex, _secondEmitterIndex;
	private final int _numOfJoinWorkersTasks;
	private boolean _isFinalAckReceived = false;
	private boolean _isFinalAckSent = false;
	private int _numParentTasks;
	private int _numRemainingParentsForLastAck;
	private int _numRemainingParentsForDMEOF; /*
											 * Number of parents (joiners) for
											 * Data migration Ended .. a
											 * reshuffler should receive DMEOF
											 * signal from all joiners to signal
											 * the end of data migration
											 */
	private final Map _conf;
	private state _currentState = state.NORMAL;
	private final StormEmitter _firstEmitter, _secondEmitter;
	private String _ID;
	private String _componentName;
	private final int _reshufflerParallelism;
	private transient InputDeclarer _currentBolt;
	private String _joinerID;
	private List<Integer> _taskMapping;
	private int[] renamings; /*
							 * Index Renamings of joiners, as joiner indexes
							 * (TaskIDs) change after mapping changes
							 */
	private Action _currentAction = null; /* action to change mapping */
	private final String _initialDim;
	private String _currentDimExcDis = "N/A"; // gets a value of N/A if no
	// migration has happened, 1-2
	// if rel 1 does Exhchange and
	// rel 2 does discards, and vice
	// versa
	private int _taskID;
	private int _taskIDIndex;
	private InterchangingComponent _interComp = null;
	private int _currentEpochNumber = 0;

	private List<Integer> _resufflerIndex;

	public boolean isInterchanging = false;
	private int _currentInterchaningCursor = -1;

	private long _numTuplesLastRelChange = 0;

	/**
	 * CLASS MEMBERS OF THE ADVISOR-COORDINATOR (DECISION MAKER) assigned to
	 * TASKIDINDEX=0
	 */
	private int _currentNumberOfAckedJoinWorkersTasks; /*
														 * Used for acking from
														 * joiners to permit
														 * change mapping i.e.,
														 * you cannot change
														 * mapping while one is
														 * being changed
														 */
	private Advisor _mAdvisor;
	private Action _AdvisorcurrentAction = null;
	private state _currentAdvisorState = state.NORMAL;

	private int _hierarchyPosition;

	public ThetaReshufflerAdvisedEpochs(StormEmitter firstRelation, StormEmitter secondRelation,
			List<String> allCompNames, int numberOfJoinWorkersTasks, int hierarchyPosition,
			Map conf, TopologyBuilder builder, String initialDim) {
		_firstEmitter = firstRelation;
		_secondEmitter = secondRelation;

		_hierarchyPosition = hierarchyPosition;
		if (_hierarchyPosition == StormComponent.FINAL_COMPONENT)
			_hierarchyPosition = StormComponent.NEXT_TO_LAST_COMPONENT;

		if (secondRelation == null) { // then first has to be of type
			// Interchanging Emitter
			_firstEmitterIndex = String.valueOf(allCompNames.indexOf(_firstEmitter.getName().split(
					"-")[0]));
			_secondEmitterIndex = String.valueOf(allCompNames.indexOf(_firstEmitter.getName()
					.split("-")[1]));
			_componentName = _firstEmitter.getName().split("-")[0] + "_"
					+ _firstEmitter.getName().split("-")[1];
			_ID = _componentName + "_RESHUFFLER";

		} else {
			_firstEmitterIndex = String.valueOf(allCompNames.indexOf(_firstEmitter.getName()));
			_secondEmitterIndex = String.valueOf(allCompNames.indexOf(_secondEmitter.getName()));
			_ID = firstRelation.getName() + "_" + secondRelation.getName() + "_RESHUFFLER";
			_componentName = firstRelation.getName() + "_" + secondRelation.getName();
		}
		_numOfJoinWorkersTasks = numberOfJoinWorkersTasks;
		_numRemainingParentsForDMEOF = numberOfJoinWorkersTasks;
		_conf = conf;
		_initialDim = initialDim;

		_reshufflerParallelism = SystemParameters.getInt(conf, _componentName + "_RESHUF_PAR");
		_currentBolt = builder.setBolt(String.valueOf(_ID), this, _reshufflerParallelism);

		renamings = new int[numberOfJoinWorkersTasks];
		for (int i = 0; i < renamings.length; i++)
			renamings[i] = i;

		final int[] initialDims = ThetaJoinUtilities.getDimensions(_initialDim);
		_mAdvisor = new TheoreticalAdvisorNew(numberOfJoinWorkersTasks, initialDims[0],
				initialDims[1], _conf);

		// **************************
		// TODO change
		if (SystemParameters.getString(conf, "DIP_QUERY_NAME").equalsIgnoreCase(
				"orders_line_fluctuations_join"))
			isInterchanging = true;

	}

	private void appendTimestampMigration(Tuple stormTupleRcv, Values tplSend) {
		if (MyUtilities.isCustomTimestampMode(_conf)) {
			final long timestamp = stormTupleRcv.getLongByField(StormComponent.TIMESTAMP);
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
	 * The Reshuffler can send: 1) Normal Datastream tuples (from previous
	 * layers) to the ThetaJoiner 2) Migrated Datastream tuples (from the
	 * ThetaJoinerDynamic) 3) Aggregated Counts to the ThetaMappingAssigner 4)
	 * Signal (stop, proceed(migrate data), DataMigrationEnded) /Mapping to the
	 * ThetaJoinerDynamic
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
		declarer.declareStream(SystemParameters.ThetaDataReshufflerToJoiner, true, new Fields(
				dataStreamFields)); // direct streams

		// 2) Theta Datastream tuples, migrated data
		final ArrayList<String> thetaStreamFields = new ArrayList<String>();
		thetaStreamFields.add(StormComponent.COMP_INDEX);
		thetaStreamFields.add(StormComponent.TUPLE);
		thetaStreamFields.add(StormComponent.HASH);
		thetaStreamFields.add(StormComponent.EPOCH);
		thetaStreamFields.add(StormComponent.DIM);
		if (MyUtilities.isCustomTimestampMode(_conf))
			thetaStreamFields.add(StormComponent.TIMESTAMP);
		declarer.declareStream(SystemParameters.ThetaDataMigrationReshufflerToJoiner, true,
				new Fields(thetaStreamFields)); // direct streams

		// 3) Signal to Joiners
		declarer.declareStream(SystemParameters.ThetaReshufflerSignal, new Fields(
				StormComponent.RESH_SIGNAL, StormComponent.MAPPING));

		/**
		 * Advisor related
		 */
		// 4)
		declarer.declareStream(SystemParameters.ThetaSynchronizerSignal, new Fields(
				StormComponent.RESH_SIGNAL, StormComponent.MAPPING));

	}

	/**
	 * The ThetaReshuffler can receive: 1) Tick signal from the clock. 2)
	 * Datastream tuples from previous relations (stages). 3) Datastream tuples
	 * from the ThetaJoiner. (migrated data from the next stage) 4) Signals
	 * (Stop or Proceed or DataMigrationEnded) and might contain new "Mapping"
	 * along side from the ThetaMappingAssigner.
	 */
	@Override
	public void execute(Tuple stormTupleRcv) {

		final String inputStream = stormTupleRcv.getSourceStreamId();

		// Case 2) Datastream tuples from previous levels
		if (inputStream.equals(SystemParameters.DATA_STREAM)) {
			final String inputComponentIndex = stormTupleRcv
					.getStringByField(StormComponent.COMP_INDEX);

			final List<String> tupleList = (List<String>) stormTupleRcv
					.getValueByField(StormComponent.TUPLE);
			final String inputTupleString = MyUtilities.tupleToString(tupleList, _conf); // INPUT
			// TUPLE
			// FINALACK~ end has reached
			if (MyUtilities.isFinalAck(tupleList, _conf)) {
				_numRemainingParentsForLastAck--;
				LOG.info(_componentName + " :Reshuffler received one LAST_ACK remaining:"
						+ _numRemainingParentsForLastAck);
				if (_numRemainingParentsForLastAck == 0)
					if (_currentState == state.FLUSHING || _currentState == state.DATAMIGRATING)
						_isFinalAckReceived = true; // final ack has been
					// received but not sent
					else {
						_isFinalAckReceived = true;
						_isFinalAckSent = true;
						MyUtilities
								.processFinalAck(_numRemainingParentsForLastAck,
										StormComponent.INTERMEDIATE, _conf, stormTupleRcv,
										_collector, null);
						if (_taskIDIndex == 0)
							LOG.info(_componentName + ":" + _taskID
									+ " :Reshuffler sending LAST_ACK.....1: ("
									+ (_currentFirstRelationCount * _reshufflerParallelism) + ","
									+ (_currentSecondRelationCount * _reshufflerParallelism) + ")");
					}
				_collector.ack(stormTupleRcv);
				return;
			}
			final String inputTupleHash = stormTupleRcv.getStringByField(StormComponent.HASH);
			// 1)increment the corresponding relation counter.
			// 2)Forward the tuple to the meant region (emit direct)
			int[] currDim;
			if (_currentAction == null)
				currDim = ThetaJoinUtilities.getDimensions(_initialDim);
			else
				currDim = new int[] { _currentAction.getNewRows(), _currentAction.getNewColumns() };

			if (_firstEmitterIndex.equals(inputComponentIndex)) {
				_currentFirstRelationCount++;
				if (_taskIDIndex == 0) {
					// ///***********//////////
					if (isInterchanging) {
						final long totalTuples = _currentFirstRelationCount
								+ _currentSecondRelationCount;
						if (_currentInterchaningCursor != 1) {
							LOG.info(_ID + ":" + _taskID
									+ " :Reshuffler INTERCHANGING adding at $1$: "
									+ " with epoch number:" + _currentEpochNumber + " ("
									+ _mAdvisor.totalRowTuples + "," + _mAdvisor.totalColumnTuples
									+ ")");
							_currentInterchaningCursor = 1;
							_numTuplesLastRelChange = totalTuples;
						}
					}
					// ///***********//////////
					_mAdvisor.updateTuples(1 * _reshufflerParallelism, 0);
				}
				final int[] taskIndices = Advisor.getAssignedReducers(true, _numOfJoinWorkersTasks,
						currDim[0], currDim[1]);
				for (int i = 0; i < taskIndices.length; i++) {
					final Values tplSend = new Values(inputComponentIndex,
							MyUtilities.stringToTuple(inputTupleString, _conf), inputTupleHash,
							_currentEpochNumber, _currentDimExcDis);
					appendTimestampNew(tplSend);
					_collector.emitDirect(_taskMapping.get(foo(renamings, taskIndices[i])),
							SystemParameters.ThetaDataReshufflerToJoiner, tplSend);
				}
			} else if (_secondEmitterIndex.equals(inputComponentIndex)) {
				_currentSecondRelationCount++;
				if (_taskIDIndex == 0) {
					// ///***********//////////
					if (isInterchanging) {
						final long totalTuples = _currentFirstRelationCount
								+ _currentSecondRelationCount;
						if (_currentInterchaningCursor != 2) {
							// && numTuplesFromLastRelChange >
							// _numTuplesLastRelChange
							LOG.info(_ID + ":" + _taskID
									+ " :Reshuffler INTERCHANGING adding at $2$: "
									+ " with epoch number:" + _currentEpochNumber + " ("
									+ _mAdvisor.totalRowTuples + "," + _mAdvisor.totalColumnTuples
									+ ")");
							_currentInterchaningCursor = 2;
							_numTuplesLastRelChange = totalTuples;
						}
					}
					_mAdvisor.updateTuples(0, 1 * _reshufflerParallelism);
					// ///***********//////////
				}
				final int[] taskIndices = Advisor.getAssignedReducers(false,
						_numOfJoinWorkersTasks, currDim[0], currDim[1]);
				for (int i = 0; i < taskIndices.length; i++) {
					final Values tplSend = new Values(inputComponentIndex,
							MyUtilities.stringToTuple(inputTupleString, _conf), inputTupleHash,
							_currentEpochNumber, _currentDimExcDis);
					appendTimestampNew(tplSend);
					_collector.emitDirect(_taskMapping.get(foo(renamings, taskIndices[i])),
							SystemParameters.ThetaDataReshufflerToJoiner, tplSend);
				}
			}

			/**
			 * ADVISOR TASKID=0 UPDATES STATS
			 */
			if (_taskIDIndex == 0 && !_isFinalAckReceived) {
				final Maybe<Action> isAction = _mAdvisor.advise();
				if (!isAction.isNone())
					if (_currentAdvisorState == state.DATAMIGRATING
							|| _currentAdvisorState == state.FLUSHING)// there
					// is a
					// pending
					// new
					// Assignment
					{
					} else {
						_AdvisorcurrentAction = isAction.get();
						_currentAdvisorState = state.FLUSHING;
						LOG.info(_componentName + ":" + _taskIDIndex
								+ " MAIN: sending new mapping:"
								+ _AdvisorcurrentAction.getNewRows() + ","
								+ _AdvisorcurrentAction.getNewColumns() + " Relation sizes "
								+ _currentFirstRelationCount + "," + _currentSecondRelationCount);

						for (int i = 1; i < _resufflerIndex.size(); i++)
							_collector.emitDirect(_resufflerIndex.get(i),
									SystemParameters.ThetaSynchronizerSignal,
									new Values(SystemParameters.ThetaSignalStop,
											_AdvisorcurrentAction.toString())); // emit
						// the
						// stop
						// signal
						// with
						// mapping
						_mAdvisor.updateDimensions(_AdvisorcurrentAction);
						// **************************Local changes
						_currentState = state.FLUSHING;
						_currentEpochNumber++;
						_currentAction = _AdvisorcurrentAction;

						final int exchgDim = identifyDim(_currentAction.getPreviousRows(),
								_currentAction.getPreviousColumns(), _currentAction.getNewRows(),
								_currentAction.getNewColumns(), false);
						final int disDim = (exchgDim == 1) ? 2 : 1;
						_currentDimExcDis = exchgDim + "-" + disDim;

						LOG.info(_ID + ":" + _taskID
								+ " :Reshuffler (sending STOP) & current mapping changing to :("
								+ _currentAction.getNewRows() + ","
								+ _currentAction.getNewColumns() + ") with epoch number:"
								+ _currentEpochNumber + " (" + _mAdvisor.totalRowTuples + ","
								+ _mAdvisor.totalColumnTuples + ")");
						updateRenamings();
						_collector
								.emit(SystemParameters.ThetaReshufflerSignal,
										new Values(SystemParameters.ThetaSignalStop,
												_AdvisorcurrentAction.toString()));
						// ///*****************************
					}
			}
		}
		/**
		 * ADVISOR only at TASKID=0
		 */
		else if (inputStream.equals(SystemParameters.ThetaJoinerAcks)) {
			final String AckMessage = stormTupleRcv.getStringByField(StormComponent.MESSAGE);
			if (AckMessage.equals(SystemParameters.ThetaAckDataMigrationEnded)) {
				// LOG.info(_componentName+" :synchronizer Ack Datamigration ended received: "+_currentNumberOfAckedJoinWorkersTasks);
				// 1)increment the _currentNumberOfAckedJoinWorkersTasks
				_currentNumberOfAckedJoinWorkersTasks++;
				// 2)if all acks have been received (equal to the number of
				// ThetaJoinerDynamic tasks) send the
				// ThetaSignalDataMigrationEnded signal & zero the value of
				// _currentNumberOfAckedJoinWorkersTasks
				if (_currentNumberOfAckedJoinWorkersTasks == _numOfJoinWorkersTasks) {
					_currentAdvisorState = state.NORMAL; // change state
					_currentNumberOfAckedJoinWorkersTasks = 0; // reset;
					LOG.info(_componentName + " :synchronizer emitting Datamigration ended"
							+ " with epoch number:" + _currentEpochNumber + " ("
							+ _mAdvisor.totalRowTuples + "," + _mAdvisor.totalColumnTuples + ")");
					for (int i = 1; i < _resufflerIndex.size(); i++)
						_collector.emitDirect(_resufflerIndex.get(i),
								SystemParameters.ThetaSynchronizerSignal, new Values(
										SystemParameters.ThetaSignalDataMigrationEnded, "N/A")); // emit
					// the
					// proceed
					// signal
					// **************************Local changes
					_currentState = state.NORMAL;
					// check if isFinalAcked=true --> emit LAST_ACK
					if (_isFinalAckReceived && !_isFinalAckSent) {
						LOG.info(_ID
								+ ":"
								+ _taskID
								+ " Reshuffler received ThetaSignalDataMigrationEnded & sent LAST_ACK ("
								+ (_currentFirstRelationCount * _reshufflerParallelism) + ","
								+ (_currentSecondRelationCount * _reshufflerParallelism) + ")");
						MyUtilities
								.processFinalAck(_numRemainingParentsForLastAck,
										StormComponent.INTERMEDIATE, _conf, stormTupleRcv,
										_collector, null);
					}
				}
			}
		}

		// Case 3) Datastream tuples from the ThetaJoiner. (Migration)
		else if (inputStream.equals(SystemParameters.ThetaDataMigrationJoinerToReshuffler)) {
			final String inputComponentIndex = stormTupleRcv
					.getStringByField(StormComponent.COMP_INDEX);

			final List<String> tupleList = (List<String>) stormTupleRcv
					.getValueByField(StormComponent.TUPLE);
			final String inputTupleString = MyUtilities.tupleToString(tupleList, _conf); // INPUT
			// TUPLE
			final String inputTupleHash = stormTupleRcv.getStringByField(StormComponent.HASH); // Hash
			// Tuple

			// If ThetaJoinerMigrationSignal, send a signal back to reshuffle
			// the next batch.
			if (inputTupleString.equals(SystemParameters.ThetaJoinerMigrationSignal)) {
				final int sentFromTaskId = stormTupleRcv.getSourceTask();
				final Values tplSend = new Values("N/A", MyUtilities.stringToTuple(
						SystemParameters.ThetaJoinerMigrationSignal, _conf), "N/A", -1, "N/A");
				appendTimestampMigration(stormTupleRcv, tplSend);
				_collector.emitDirect(sentFromTaskId,
						SystemParameters.ThetaDataMigrationReshufflerToJoiner, tplSend);
				_collector.ack(stormTupleRcv);
				return;
			}
			// if its a DMEOF then send signal dataMigrationEnded to all the
			// joiners
			else if (inputTupleString.equals(SystemParameters.ThetaJoinerDataMigrationEOF)) {
				_numRemainingParentsForDMEOF--;
				// LOG.info(_componentName+":"+_taskID+" :Reshuffler received one DMEOF remaining:"+_numRemainingParentsForDMEOF+" Buffered Storage:"+bufferedTuples.size());
				if (_numRemainingParentsForDMEOF == 0) {
					LOG.info(_componentName + " :Reshuffler emitting received last DEMOF");
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
				return;
			}

			// in this case tuples received should have the new epoch number
			// Assertion
			if (!stormTupleRcv.getIntegerByField(StormComponent.EPOCH).equals(_currentEpochNumber))
				LOG.error("Error --> Data migrated tuple has sent an outdated epoch number");

			// 1) Dont increment the counters. (this is only data migration)
			// 2) Forward the tuple to the meant (new) region.
			if (_firstEmitterIndex.equals(inputComponentIndex)) {
				final int sourceID = renamings[_taskMapping.indexOf(stormTupleRcv.getSourceTask())];
				final int[] taskIndices = _currentAction.getRowExchangeReducersByNewId(sourceID);
				for (int i = 0; i < taskIndices.length; i++) {
					final int index = foo(renamings, taskIndices[i]);
					final Values tplSend = new Values(inputComponentIndex,
							MyUtilities.stringToTuple(inputTupleString, _conf), inputTupleHash,
							_currentEpochNumber, _currentDimExcDis);
					appendTimestampMigration(stormTupleRcv, tplSend);
					_collector.emitDirect(_taskMapping.get(index),
							SystemParameters.ThetaDataMigrationReshufflerToJoiner, tplSend);
				}
			} else if (_secondEmitterIndex.equals(inputComponentIndex)) {
				final int sourceID = renamings[_taskMapping.indexOf(stormTupleRcv.getSourceTask())];
				final int[] taskIndices = _currentAction.getColumnExchangeReducersByNewId(sourceID);
				for (int i = 0; i < taskIndices.length; i++) {
					final Values tplSend = new Values(inputComponentIndex,
							MyUtilities.stringToTuple(inputTupleString, _conf), inputTupleHash,
							_currentEpochNumber, _currentDimExcDis);
					appendTimestampMigration(stormTupleRcv, tplSend);
					_collector.emitDirect(_taskMapping.get(foo(renamings, taskIndices[i])),
							SystemParameters.ThetaDataMigrationReshufflerToJoiner, tplSend);
				}
			}
		}
		// Case 4) Signals (Stop or Proceed or DataMigrationEnded) and might
		// contain new "Mapping" along side. from the ThetaMappingAssigner.
		else if (inputStream.equals(SystemParameters.ThetaSynchronizerSignal)) {
			final String mapping = stormTupleRcv.getStringByField(StormComponent.MAPPING);
			final String signal = stormTupleRcv.getStringByField(StormComponent.RESH_SIGNAL);

			// - if Signal is Stop, set the new mapping
			if (signal.equals(SystemParameters.ThetaSignalStop)) {
				_currentState = state.FLUSHING;
				final String actionString = mapping;
				_currentEpochNumber++;
				_currentAction = Action.fromString(actionString);

				final int exchgDim = identifyDim(_currentAction.getPreviousRows(),
						_currentAction.getPreviousColumns(), _currentAction.getNewRows(),
						_currentAction.getNewColumns(), false);
				final int disDim = (exchgDim == 1) ? 2 : 1;
				_currentDimExcDis = exchgDim + "-" + disDim;

				LOG.info(_ID + ":" + _taskID
						+ " :Reshuffler (sending STOP) & current mapping changing to : "
						+ _currentAction.getNewRows() + "-" + _currentAction.getNewColumns()
						+ " with epoch number:" + _currentEpochNumber);
				updateRenamings();
				_collector.emit(SystemParameters.ThetaReshufflerSignal, new Values(
						SystemParameters.ThetaSignalStop, actionString));
			}
			// - if Signal is DataMigration phase has ended
			else if (signal.equals(SystemParameters.ThetaSignalDataMigrationEnded)) {
				LOG.info(_ID + ":" + _taskID + " Reshuffler received ThetaSignalDataMigrationEnded");
				_currentState = state.NORMAL;
				// check if isFinalAcked=true --> emit LAST_ACK
				if (_isFinalAckReceived && !_isFinalAckSent) {
					LOG.info(_ID + ":" + _taskID
							+ " Reshuffler received ThetaSignalDataMigrationEnded & sent LAST_ACK");
					MyUtilities.processFinalAck(_numRemainingParentsForLastAck,
							StormComponent.INTERMEDIATE, _conf, stormTupleRcv, _collector, null);
				}
			}
		} else
			LOG.info(_ID + ":" + _taskID + " ERROR: WRONG STREAM ID!!!");
		_collector.ack(stormTupleRcv);
	}

	/* For renamings */
	private int foo(int[] renamings2, int k) {
		for (int i = 0; i < renamings2.length; ++i)
			if (renamings2[i] == k)
				return i;
		return -1;
	}

	public InterchangingComponent get_interComp() {
		return _interComp;
	}

	public InputDeclarer getCurrentBolt() {
		return _currentBolt;
	}

	public String getID() {
		return _ID;
	}

	public String getInfoID() {
		final String str = "ThetaReshuffler " + _componentName + "_RESHUF" + " has ID: " + _ID;
		return str;
	}

	public int getReshufflerParallelism() {
		return _reshufflerParallelism;
	}

	private int identifyDim(int prev1, int prev2, int curr1, int curr2, boolean isDiscarding) {

		return ThetaJoinerDynamicAdvisedEpochs.identifyDim(prev1, prev2, curr1, curr2,
				isDiscarding);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_taskID = context.getThisTaskId();
		_taskIDIndex = context.getThisTaskIndex();

		_resufflerIndex = context.getComponentTasks(getID());

		if (_taskIDIndex != 0)
			_mAdvisor = null;
		_taskMapping = context.getComponentTasks(_joinerID);
		_collector = collector;
		if (_secondEmitter == null) // this is an interchanging data source
			_numParentTasks = 1;

		else if (_interComp == null)
			_numParentTasks = MyUtilities.getNumParentTasks(context, _firstEmitter, _secondEmitter);
		else
			_numParentTasks = MyUtilities.getNumParentTasks(context, _interComp);
		_numRemainingParentsForLastAck = _numParentTasks;
	}

	public void set_interComp(InterchangingComponent _interComp) {
		this._interComp = _interComp;
	}

	public void setJoinerID(String _joinerID) {
		this._joinerID = _joinerID;
	}

	private void updateRenamings() {
		final int temp[] = new int[renamings.length];
		for (int i = 0; i < renamings.length; i++)
			temp[i] = _currentAction.getNewReducerName(renamings[i]);
		renamings = temp;
	}

}
