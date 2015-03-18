package ch.epfl.data.plan_runner.storm_components;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import ch.epfl.data.plan_runner.components.ComponentProperties;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.operators.Operator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.storage.BasicStore;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.PeriodicAggBatchSend;
import ch.epfl.data.plan_runner.utilities.SystemParameters;

@Deprecated
public class StormSrcStorage extends StormBoltComponent {
	private static Logger LOG = Logger.getLogger(StormSrcStorage.class);
	private static final long serialVersionUID = 1L;

	private final String _tableName;
	private final boolean _isFromFirstEmitter; // receive R updates

	// a unique id of the components which sends to StormSrcJoin
	private final String _firstEmitterIndex, _secondEmitterIndex;
	private final List<Integer> _rightHashIndexes; // hashIndexes from the right
	// parent

	private final String _full_ID; // Contains the name of the component and
									// tableName

	private final ChainOperator _operatorChain;

	private final BasicStore<ArrayList<String>> _joinStorage;
	private final ProjectOperator _preAggProj;
	private final StormSrcHarmonizer _harmonizer;

	private int _numSentTuples;

	// for batch sending
	private final Semaphore _semAgg = new Semaphore(1, true);
	private boolean _firstTime = true;
	private PeriodicAggBatchSend _periodicAggBatch;
	private final long _batchOutputMillis;

	public StormSrcStorage(StormEmitter firstEmitter,
			StormEmitter secondEmitter, ComponentProperties cp,
			List<String> allCompNames, StormSrcHarmonizer harmonizer,
			boolean isFromFirstEmitter,
			BasicStore<ArrayList<String>> preAggStorage,
			ProjectOperator preAggProj, int hierarchyPosition,
			TopologyBuilder builder, TopologyKiller killer, Config conf) {
		super(cp, allCompNames, hierarchyPosition, conf);

		_firstEmitterIndex = String.valueOf(allCompNames.indexOf(firstEmitter
				.getName()));
		_secondEmitterIndex = String.valueOf(allCompNames.indexOf(secondEmitter
				.getName()));

		_tableName = (isFromFirstEmitter ? firstEmitter.getName()
				: secondEmitter.getName());
		_isFromFirstEmitter = isFromFirstEmitter;
		_harmonizer = harmonizer;
		_batchOutputMillis = cp.getBatchOutputMillis();

		_operatorChain = cp.getChainOperator();
		_rightHashIndexes = cp.getParents()[1].getHashIndexes();

		final int parallelism = SystemParameters.getInt(conf, getID() + "_PAR");
		_full_ID = getID() + "_" + _tableName;
		final InputDeclarer currentBolt = builder.setBolt(_full_ID, this,
				parallelism);
		currentBolt.fieldsGrouping(_harmonizer.getID(), new Fields("Hash"));

		if (getHierarchyPosition() == FINAL_COMPONENT
				&& (!MyUtilities.isAckEveryTuple(conf)))
			killer.registerComponent(this, parallelism);

		if (cp.getPrintOut() && _operatorChain.isBlocking())
			currentBolt.allGrouping(killer.getID(),
					SystemParameters.DUMP_RESULTS_STREAM);

		_joinStorage = preAggStorage;
		_preAggProj = preAggProj;
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
						tupleSend(MyUtilities.stringToTuple(tuple, getConf()),
								null, 0);
					// clearing
					agg.clearStorage();
					_semAgg.release();
				}
			}
	}

	private void applyOperatorsAndSend(Tuple stormTupleRcv, List<String> tuple) {
		if (MyUtilities.isAggBatchOutputMode(_batchOutputMillis))
			try {
				_semAgg.acquire();
			} catch (final InterruptedException ex) {
			}
		tuple = _operatorChain.process(tuple, 0);
		if (MyUtilities.isAggBatchOutputMode(_batchOutputMillis))
			_semAgg.release();

		if (tuple == null)
			return;
		_numSentTuples++;
		printTuple(tuple);

		if (MyUtilities.isSending(getHierarchyPosition(), _batchOutputMillis))
			if (MyUtilities.isCustomTimestampMode(getConf()))
				tupleSend(tuple, stormTupleRcv, stormTupleRcv.getLong(3));
			else
				tupleSend(tuple, stormTupleRcv, 0);
		if (MyUtilities.isPrintLatency(getHierarchyPosition(), getConf()))
			printTupleLatency(_numSentTuples - 1, stormTupleRcv.getLong(3));
	}

	// from IRichBolt
	@Override
	public void execute(Tuple stormTupleRcv) {
		if (_firstTime && MyUtilities.isAggBatchOutputMode(_batchOutputMillis)) {
			_periodicAggBatch = new PeriodicAggBatchSend(_batchOutputMillis,
					this);
			_firstTime = false;
		}

		if (receivedDumpSignal(stormTupleRcv)) {
			MyUtilities.dumpSignal(this, stormTupleRcv, getCollector());
			return;
		}

		// inside StormSrcJoin, we have inputComponentName, not
		// inputComponentIndex
		final String inputComponentIndex = stormTupleRcv.getString(0);
		final List<String> tuple = (List<String>) stormTupleRcv.getValue(1);
		final String inputTupleString = MyUtilities.tupleToString(tuple,
				getConf());
		final String inputTupleHash = stormTupleRcv.getString(2);

		if (processFinalAck(tuple, stormTupleRcv))
			return;

		if ((_isFromFirstEmitter && (inputComponentIndex
				.equals(_firstEmitterIndex)))
				|| (!_isFromFirstEmitter && (inputComponentIndex
						.equals(_secondEmitterIndex))))
			// the
			// tuple
			// into
			// the
			// datastructure!!
			_joinStorage.insert(inputTupleHash, inputTupleString);
		else {// JOIN
			final List<String> oppositeTupleStringList = _joinStorage
					.access(inputTupleHash);

			// do stuff
			if (oppositeTupleStringList != null)
				for (int i = 0; i < oppositeTupleStringList.size(); i++) {
					final String oppositeTupleString = oppositeTupleStringList
							.get(i);
					final List<String> oppositeTuple = MyUtilities
							.stringToTuple(oppositeTupleString, getConf());

					List<String> firstTuple, secondTuple;
					if (_isFromFirstEmitter) {
						// we receive R updates in the Storage which is
						// responsible for S
						firstTuple = oppositeTuple;
						secondTuple = tuple;
					} else {
						firstTuple = tuple;
						secondTuple = oppositeTuple;
					}

					List<String> outputTuple;
					if (_joinStorage instanceof BasicStore)
						outputTuple = MyUtilities.createOutputTuple(firstTuple,
								secondTuple, _rightHashIndexes);
					else
						outputTuple = MyUtilities.createOutputTuple(firstTuple,
								secondTuple);

					if (_preAggProj != null)
						outputTuple = _preAggProj.process(outputTuple, 0);

					applyOperatorsAndSend(stormTupleRcv, outputTuple);
				}
		}
		getCollector().ack(stormTupleRcv);
	}

	@Override
	public ChainOperator getChainOperator() {
		return _operatorChain;
	}

	@Override
	public String getInfoID() {
		final String str = "SourceStorage " + _full_ID + " has ID: " + getID();
		return str;
	}

	@Override
	protected InterchangingComponent getInterComp() {
		// should never be invoked
		return null;
	}

	// from StormEmitter interface
	@Override
	public String getName() {
		return _full_ID;
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
	public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
		super.prepare(map, tc, collector);
		super.setNumRemainingParents(MyUtilities.getNumParentTasks(tc,
				_harmonizer));
	}

	@Override
	protected void printStatistics(int type) {
		// TODO
	}

	@Override
	public void purgeStaleStateFromWindow() {
		throw new RuntimeException(
				"Window semantics is not implemented for this operator.");

	}
}
