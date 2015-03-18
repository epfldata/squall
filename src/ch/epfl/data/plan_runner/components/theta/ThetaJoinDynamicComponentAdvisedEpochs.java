/**
 *
 * @author El Seidy
 * This Class is the Theta-join-Dynamic Wrapper which includes all the Theta components.
 * 1- ThetaReshuffler Bolt.
 * 2- ThetaJoiner Bolt.
 * 3- One instance of ThetaClock Spout.
 * 4- One instance of ThetaMappingAssignerSynchronizer Bolt.
 */
package ch.epfl.data.plan_runner.components.theta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.components.JoinerComponent;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.operators.Operator;
import ch.epfl.data.plan_runner.predicates.Predicate;
import ch.epfl.data.plan_runner.storm_components.InterchangingComponent;
import ch.epfl.data.plan_runner.storm_components.StormComponent;
import ch.epfl.data.plan_runner.storm_components.StormEmitter;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.thetajoin.dynamic.storm_component.ThetaJoinerDynamicAdvisedEpochs;
import ch.epfl.data.plan_runner.thetajoin.dynamic.storm_component.ThetaReshufflerAdvisedEpochs;
import ch.epfl.data.plan_runner.thetajoin.matrix_mapping.EquiMatrixAssignment;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.SystemParameters;
import ch.epfl.data.plan_runner.utilities.thetajoin_dynamic.ThetaDataMigrationJoinerToReshufflerMapping;
import ch.epfl.data.plan_runner.utilities.thetajoin_dynamic.ThetaJoinDynamicMapping;
import ch.epfl.data.plan_runner.window_semantics.WindowSemanticsManager;

public class ThetaJoinDynamicComponentAdvisedEpochs extends JoinerComponent implements Component {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger
			.getLogger(ThetaJoinDynamicComponentAdvisedEpochs.class);
	private final Component _firstParent;
	private final Component _secondParent;
	private Component _child;
	private String _componentName;
	private long _batchOutputMillis;
	private List<Integer> _hashIndexes;
	private List<ValueExpression> _hashExpressions;
	private ThetaJoinerDynamicAdvisedEpochs _joiner;
	private ThetaReshufflerAdvisedEpochs _reshuffler;
	private final ChainOperator _chain = new ChainOperator();
	private boolean _printOut;
	private boolean _printOutSet; // whether printOut was already set
	private Predicate _joinPredicate;
	private int _joinerParallelism;
	private InterchangingComponent _interComp = null;

	public ThetaJoinDynamicComponentAdvisedEpochs(Component firstParent,
			Component secondParent) {
		_firstParent = firstParent;
		_firstParent.setChild(this);
		_secondParent = secondParent;
		if (_secondParent != null) {
			_secondParent.setChild(this);
			_componentName = firstParent.getName() + "_"
					+ secondParent.getName();
		} else
			_componentName = new String(firstParent.getName().split("-")[0])
					+ "_" + new String(firstParent.getName().split("-")[1]);
	}

	@Override
	public ThetaJoinDynamicComponentAdvisedEpochs add(Operator operator) {
		_chain.addOperator(operator);
		return this;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Component)
			return _componentName.equals(((Component) obj).getName());
		else
			return false;
	}

	@Override
	public List<DataSourceComponent> getAncestorDataSources() {
		final List<DataSourceComponent> list = new ArrayList<DataSourceComponent>();
		for (final Component parent : getParents())
			list.addAll(parent.getAncestorDataSources());
		return list;
	}

	@Override
	public long getBatchOutputMillis() {
		return _batchOutputMillis;
	}

	@Override
	public ChainOperator getChainOperator() {
		return _chain;
	}

	@Override
	public Component getChild() {
		return _child;
	}

	// from StormEmitter interface
	@Override
	public String[] getEmitterIDs() {
		return _joiner.getEmitterIDs();
	}

	@Override
	public List<String> getFullHashList() {
		throw new RuntimeException(
				"Load balancing for Dynamic Theta join is done inherently!");
	}

	@Override
	public List<ValueExpression> getHashExpressions() {
		return _hashExpressions;
	}

	@Override
	public List<Integer> getHashIndexes() {
		return _hashIndexes;
	}

	@Override
	public String getInfoID() {
		return _joiner.getInfoID();
	}

	public InterchangingComponent getInterComp() {
		return _interComp;
	}

	public Predicate getJoinPredicate() {
		return _joinPredicate;
	}

	@Override
	public String getName() {
		return _componentName;
	}

	@Override
	public Component[] getParents() {
		return new Component[] { _firstParent, _secondParent };
	}

	@Override
	public boolean getPrintOut() {
		return _printOut;
	}

	@Override
	public int hashCode() {
		int hash = 7;
		hash = 37 * hash
				+ (_componentName != null ? _componentName.hashCode() : 0);
		return hash;
	}

	@Override
	public void makeBolts(TopologyBuilder builder, TopologyKiller killer,
			List<String> allCompNames, Config conf, int hierarchyPosition) {

		// by default print out for the last component
		// for other conditions, can be set via setPrintOut
		if (hierarchyPosition == StormComponent.FINAL_COMPONENT
				&& !_printOutSet)
			setPrintOut(true);
		_joinerParallelism = SystemParameters.getInt(conf, _componentName
				+ "_PAR");
		MyUtilities.checkBatchOutput(_batchOutputMillis,
				_chain.getAggregation(), conf);

		int firstCardinality, secondCardinality;
		if (_secondParent == null) { // then first has to be of type
			// Interchanging Emitter
			firstCardinality = SystemParameters.getInt(conf, new String(
					_firstParent.getName().split("-")[0] + "_CARD"));
			secondCardinality = SystemParameters.getInt(conf, new String(
					_firstParent.getName().split("-")[1] + "_CARD"));
		} else {
			firstCardinality = SystemParameters.getInt(conf,
					_firstParent.getName() + "_CARD");
			secondCardinality = SystemParameters.getInt(conf,
					_secondParent.getName() + "_CARD");
		}

		final EquiMatrixAssignment _currentMappingAssignment = new EquiMatrixAssignment(
				firstCardinality, secondCardinality, _joinerParallelism, -1);

		final String dim = _currentMappingAssignment.getMappingDimensions();
		// dim ="1-1"; //initiate Splitting
		LOG.info(_componentName + "Initial Dimensions is: " + dim);

		// create the bolts ..

		// Create the reshuffler.
		_reshuffler = new ThetaReshufflerAdvisedEpochs(_firstParent,
				_secondParent, allCompNames, _joinerParallelism,
				hierarchyPosition, conf, builder, dim);

		if (_interComp != null)
			_reshuffler.set_interComp(_interComp);

		// Create the Join Bolt.
		_joiner = new ThetaJoinerDynamicAdvisedEpochs(_firstParent,
				_secondParent, this, allCompNames, _joinPredicate,
				hierarchyPosition, builder, killer, conf, _reshuffler, dim);
		_reshuffler.setJoinerID(_joiner.getID());

		/*
		 * setup communication between the components.
		 */
		// 1) Hook up the mapper&Synchronizer(reshuffler) to the reshuffler
		_reshuffler.getCurrentBolt().directGrouping(_reshuffler.getID(),
				SystemParameters.ThetaSynchronizerSignal);

		// 2) Hook up the previous emitters to the reshuffler

		final ThetaJoinDynamicMapping dMap = new ThetaJoinDynamicMapping(conf,
				-1);
		final ArrayList<StormEmitter> emittersList = new ArrayList<StormEmitter>();
		if (_interComp == null) {
			emittersList.add(_firstParent);
			if (_secondParent != null)
				emittersList.add(_secondParent);
		} else
			emittersList.add(_interComp);
		for (final StormEmitter emitter : emittersList) {
			final String[] emitterIDs = emitter.getEmitterIDs();
			for (final String emitterID : emitterIDs)
				_reshuffler.getCurrentBolt().customGrouping(emitterID, dMap); // default
			// message
			// stream
		}
		// 3) Hook up the DataMigration from the joiners to the reshuffler
		_reshuffler.getCurrentBolt().customGrouping(_joiner.getID(),
				SystemParameters.ThetaDataMigrationJoinerToReshuffler,
				new ThetaDataMigrationJoinerToReshufflerMapping(conf, -1));
		// --for the LAST_ACK !!
		_joiner.getCurrentBolt().allGrouping(_reshuffler.getID());
		// 4) Hook up the signals from the reshuffler to the joiners
		_joiner.getCurrentBolt().allGrouping(_reshuffler.getID(),
				SystemParameters.ThetaReshufflerSignal);
		// 5) Hook up the DataMigration from the reshuffler to the joiners
		_joiner.getCurrentBolt().directGrouping(_reshuffler.getID(),
				SystemParameters.ThetaDataMigrationReshufflerToJoiner);
		// 6) Hook up the Data_Stream data (normal tuples) from the reshuffler
		// to the joiners
		_joiner.getCurrentBolt().directGrouping(_reshuffler.getID(),
				SystemParameters.ThetaDataReshufflerToJoiner);
		// 7) Hook up the Acks from the Joiner to the Mapper&Synchronizer
		_reshuffler.getCurrentBolt().directGrouping(_joiner.getID(),
				SystemParameters.ThetaJoinerAcks);// synchronizer is already one
		// anyway.
	}

	@Override
	public ThetaJoinDynamicComponentAdvisedEpochs setBatchOutputMillis(
			long millis) {
		_batchOutputMillis = millis;
		return this;
	}

	@Override
	public void setChild(Component child) {
		_child = child;
	}

	// TODO IMPLEMENT ME
	@Override
	public Component setContentSensitiveThetaJoinWrapper(TypeConversion wrapper) {
		return this;
	}

	// list of distinct keys, used for direct stream grouping and load-balancing
	// ()
	@Override
	public ThetaJoinStaticComponent setFullHashList(List<String> fullHashList) {
		throw new RuntimeException(
				"Load balancing for Dynamic Theta join is done inherently!");
	}

	@Override
	public ThetaJoinDynamicComponentAdvisedEpochs setHashExpressions(
			List<ValueExpression> hashExpressions) {
		_hashExpressions = hashExpressions;
		return this;
	}

	@Override
	public ThetaJoinDynamicComponentAdvisedEpochs setInterComp(
			InterchangingComponent _interComp) {
		this._interComp = _interComp;
		return this;
	}

	@Override
	public Component setJoinPredicate(Predicate joinPredicate) {
		_joinPredicate = joinPredicate;
		return this;
	}

	@Override
	public ThetaJoinDynamicComponentAdvisedEpochs setOutputPartKey(
			int... hashIndexes) {
		return setOutputPartKey(Arrays.asList(ArrayUtils.toObject(hashIndexes)));
	}

	@Override
	public ThetaJoinDynamicComponentAdvisedEpochs setOutputPartKey(
			List<Integer> hashIndexes) {
		_hashIndexes = hashIndexes;
		return this;
	}

	@Override
	public ThetaJoinDynamicComponentAdvisedEpochs setPrintOut(boolean printOut) {
		_printOutSet = true;
		_printOut = printOut;
		return this;
	}

	@Override
	public Component setSlidingWindow(int windowRange) {
		WindowSemanticsManager._IS_WINDOW_SEMANTICS=true;
		_windowSize = windowRange*1000; // Width in terms of millis, Default is -1 which is full history
		
		return this;
	}

	@Override
	public Component setTumblingWindow(int windowRange) {
		WindowSemanticsManager._IS_WINDOW_SEMANTICS=true;
		_tumblingWindowSize= windowRange*1000 ;//For tumbling semantics
		return null;
	}

}
