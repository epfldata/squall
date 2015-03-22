package ch.epfl.data.plan_runner.storm_components;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

import ch.epfl.data.plan_runner.components.ComponentProperties;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.operators.Operator;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.PeriodicAggBatchSend;
import ch.epfl.data.plan_runner.utilities.SystemParameters;

public class StormOperator extends StormBoltComponent {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormOperator.class);

	private final ChainOperator _operatorChain;

	private int _numSentTuples = 0;

	// if this is set, we receive using direct stream grouping
	private final List<String> _fullHashList;

	// for agg batch sending
	private final Semaphore _semAgg = new Semaphore(1, true);
	private boolean _firstTime = true;
	private PeriodicAggBatchSend _periodicAggBatch;
	private final long _aggBatchOutputMillis;

	public StormOperator(StormEmitter parentEmitter, ComponentProperties cp,
			List<String> allCompNames, int hierarchyPosition,
			TopologyBuilder builder, TopologyKiller killer, Config conf) {
		super(cp, allCompNames, hierarchyPosition, conf);

		_aggBatchOutputMillis = cp.getBatchOutputMillis();

		final int parallelism = SystemParameters.getInt(conf, getID() + "_PAR");

		// if(parallelism > 1 && distinct != null){
		// throw new RuntimeException(_componentName +
		// ": Distinct operator cannot be specified for multiThreaded bolts!");
		// }
		_operatorChain = cp.getChainOperator();

		InputDeclarer currentBolt = builder.setBolt(getID(), this, parallelism);

		_fullHashList = cp.getFullHashList();

		if (MyUtilities.isManualBatchingMode(getConf()))
			currentBolt = MyUtilities.attachEmitterBatch(conf, _fullHashList,
					currentBolt, parentEmitter);
		else
			currentBolt = MyUtilities.attachEmitterHash(conf, _fullHashList,
					currentBolt, parentEmitter);

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
			List<String> tuple, boolean isLastInBatch) {
		if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
			try {
				_semAgg.acquire();
			} catch (final InterruptedException ex) {
			}
		tuple = _operatorChain.process(tuple);
		if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
			_semAgg.release();

		if (tuple == null) {
			getCollector().ack(stormTupleRcv);
			return;
		}
		_numSentTuples++;
		printTuple(tuple);

		if (MyUtilities
				.isSending(getHierarchyPosition(), _aggBatchOutputMillis)) {
			long timestamp = 0;
			if (MyUtilities.isCustomTimestampMode(getConf()))
				timestamp = stormTupleRcv
						.getLongByField(StormComponent.TIMESTAMP);
			tupleSend(tuple, stormTupleRcv, timestamp);
		}
		if (MyUtilities.isPrintLatency(getHierarchyPosition(), getConf())) {
			long timestamp;
			if (MyUtilities.isManualBatchingMode(getConf())) {
				if (isLastInBatch) {
					timestamp = stormTupleRcv
							.getLongByField(StormComponent.TIMESTAMP); // getLong(2);
					printTupleLatency(_numSentTuples - 1, timestamp);
				}
			} else {
				timestamp = stormTupleRcv
						.getLongByField(StormComponent.TIMESTAMP); // getLong(3);
				printTupleLatency(_numSentTuples - 1, timestamp);
			}
		}
	}

	// from IRichBolt
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
			final List<String> tuple = (List<String>) stormTupleRcv
					.getValueByField(StormComponent.TUPLE);// getValue(1);

			if (processFinalAck(tuple, stormTupleRcv))
				return;

			applyOperatorsAndSend(stormTupleRcv, tuple, true);

		} else {
			final String inputBatch = stormTupleRcv
					.getStringByField(StormComponent.TUPLE); // getString(1);

			final String[] wholeTuples = inputBatch
					.split(SystemParameters.MANUAL_BATCH_TUPLE_DELIMITER);
			final int batchSize = wholeTuples.length;
			for (int i = 0; i < batchSize; i++) {
				// parsing
				final String currentTuple = new String(wholeTuples[i]);
				final String[] parts = currentTuple
						.split(SystemParameters.MANUAL_BATCH_HASH_DELIMITER);

				String inputTupleString = null;
				if (parts.length == 1)
					// lastAck
					inputTupleString = new String(parts[0]);
				else
					inputTupleString = new String(parts[1]);
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
					applyOperatorsAndSend(stormTupleRcv, tuple, true);
				else
					applyOperatorsAndSend(stormTupleRcv, tuple, false);
			}
		}
		getCollector().ack(stormTupleRcv);
	}

	@Override
	public ChainOperator getChainOperator() {
		return _operatorChain;
	}

	// from StormComponent
	@Override
	public String getInfoID() {
		final String str = "OperatorComponent " + getID() + " has ID: "
				+ getID();
		return str;
	}

	@Override
	protected InterchangingComponent getInterComp() {
		// should never be invoked
		return null;
	}

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
		// TODO
	}
}