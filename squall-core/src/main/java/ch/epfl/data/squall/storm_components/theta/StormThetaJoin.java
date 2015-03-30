package ch.epfl.data.squall.storm_components.theta;

import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import ch.epfl.data.squall.components.ComponentProperties;
import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.storage.TupleStorage;
import ch.epfl.data.squall.storm_components.InterchangingComponent;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.StormEmitter;
import ch.epfl.data.squall.storm_components.StormJoinerBoltComponent;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.thetajoin.matrix_mapping.ContentSensitiveMatrixAssignment;
import ch.epfl.data.squall.thetajoin.matrix_mapping.EquiMatrixAssignment;
import ch.epfl.data.squall.thetajoin.matrix_mapping.MatrixAssignment;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.PeriodicAggBatchSend;
import ch.epfl.data.squall.utilities.SystemParameters;
import ch.epfl.data.squall.utilities.statistics.StatisticsUtilities;
import ch.epfl.data.squall.window_semantics.WindowSemanticsManager;

public class StormThetaJoin extends StormJoinerBoltComponent {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormThetaJoin.class);
	private final TupleStorage _firstRelationStorage, _secondRelationStorage;

	public StormThetaJoin(StormEmitter firstEmitter,
			StormEmitter secondEmitter, ComponentProperties cp,
			List<String> allCompNames, Predicate joinPredicate,
			boolean isPartitioner, int hierarchyPosition,
			TopologyBuilder builder, TopologyKiller killer, Config conf,
			InterchangingComponent interComp, boolean isContentSensitive,
			TypeConversion wrapper) {
		super(firstEmitter, secondEmitter, cp, allCompNames, joinPredicate,
				hierarchyPosition, builder, killer, isPartitioner, conf);
		_statsUtils = new StatisticsUtilities(getConf(), LOG);
		final int parallelism = SystemParameters.getInt(conf, getID() + "_PAR");
		InputDeclarer currentBolt = builder.setBolt(getID(), this, parallelism);
		final MatrixAssignment _currentMappingAssignment;
		if ((!isContentSensitive)
				|| (hierarchyPosition != StormComponent.FINAL_COMPONENT && hierarchyPosition != StormComponent.NEXT_TO_DUMMY)) {
			final int firstCardinality = SystemParameters.getInt(conf,
					firstEmitter.getName() + "_CARD");
			final int secondCardinality = SystemParameters.getInt(conf,
					secondEmitter.getName() + "_CARD");
			// regions assignment exist only for the last-level join
			_currentMappingAssignment = new EquiMatrixAssignment(
					firstCardinality, secondCardinality, parallelism, -1);
		} else {
			_currentMappingAssignment = new ContentSensitiveMatrixAssignment(
					conf); // TODO
		}
		final String dim = _currentMappingAssignment.toString();
		LOG.info(getID() + " Initial Dimensions is: " + dim);

		if (interComp == null)
			currentBolt = MyUtilities.thetaAttachEmitterComponents(currentBolt,
					firstEmitter, secondEmitter, allCompNames,
					_currentMappingAssignment, conf, wrapper);
		else {
			currentBolt = MyUtilities
					.thetaAttachEmitterComponentsWithInterChanging(currentBolt,
							firstEmitter, secondEmitter, allCompNames,
							_currentMappingAssignment, conf, interComp);
			_inter = interComp;
		}
		if (getHierarchyPosition() == FINAL_COMPONENT
				&& (!MyUtilities.isAckEveryTuple(conf)))
			killer.registerComponent(this, parallelism);
		if (cp.getPrintOut() && _operatorChain.isBlocking())
			currentBolt.allGrouping(killer.getID(),
					SystemParameters.DUMP_RESULTS_STREAM);
		_firstRelationStorage = new TupleStorage();
		_secondRelationStorage = new TupleStorage();
		if (_joinPredicate != null) {
			createIndexes();
			_existIndexes = true;
		} else
			_existIndexes = false;
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
			final String inputTupleString = MyUtilities.tupleToString(tuple,
					getConf());
			processNonLastTuple(inputComponentIndex, tuple, inputTupleHash,
					stormTupleRcv, true, _firstRelationStorage,
					_secondRelationStorage);
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
							inputTupleHash, stormTupleRcv, true,
							_firstRelationStorage, _secondRelationStorage);
				else
					processNonLastTuple(inputComponentIndex, tuple,
							inputTupleHash, stormTupleRcv, false,
							_firstRelationStorage, _secondRelationStorage);
			}
		}
		// TODO
		// Update LatestTimeStamp
		WindowSemanticsManager.updateLatestTimeStamp(this, stormTupleRcv);
		getCollector().ack(stormTupleRcv);
	}

	@Override
	protected void printStatistics(int type) {
		printStatistics(type, _firstRelationStorage.size(),
				_secondRelationStorage.size(), LOG);
	}

	// TODO WINDOW Semantics
	@Override
	public void purgeStaleStateFromWindow() {
		_firstRelationStorage.purgeState(_latestTimeStamp
				- WindowSemanticsManager._GC_PERIODIC_TICK,
				_firstRelationIndexes, _joinPredicate, getConf(), true);
		_secondRelationStorage.purgeState(_latestTimeStamp
				- WindowSemanticsManager._GC_PERIODIC_TICK,
				_secondRelationIndexes, _joinPredicate, getConf(), false);
		System.gc();
	}
}