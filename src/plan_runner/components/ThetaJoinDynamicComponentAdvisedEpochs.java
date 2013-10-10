/**
 *
 * @author El Seidy
 * This Class is the Theta-join-Dynamic Wrapper which includes all the Theta components.
 * 1- ThetaReshuffler Bolt.
 * 2- ThetaJoiner Bolt.
 * 3- One instance of ThetaClock Spout.
 * 4- One instance of ThetaMappingAssignerSynchronizer Bolt.
 */
package plan_runner.components;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import plan_runner.expressions.ValueExpression;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.Operator;
import plan_runner.predicates.Predicate;
import plan_runner.query_plans.QueryPlan;
import plan_runner.storm_components.InterchangingComponent;
import plan_runner.storm_components.StormComponent;
import plan_runner.storm_components.StormEmitter;
import plan_runner.storm_components.synchronization.TopologyKiller;
import plan_runner.thetajoin.dynamic.storm_component.New.ThetaJoinerDynamicAdvisedEpochsNew;
import plan_runner.thetajoin.dynamic.storm_component.New.ThetaReshufflerAdvisedEpochsNew;
import plan_runner.thetajoin.matrix_mapping.EquiMatrixAssignment;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.SystemParameters;
import plan_runner.utilities.ThetaDataMigrationJoinerToReshufflerMapping;
import plan_runner.utilities.ThetaJoinDynamicMapping;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class ThetaJoinDynamicComponentAdvisedEpochs implements Component {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(ThetaJoinDynamicComponentAdvisedEpochs.class);
	private final Component _firstParent;
	private final Component _secondParent;
	private Component _child;
	private String _componentName;
	private long _batchOutputMillis;
	private List<Integer> _hashIndexes;
	private List<ValueExpression> _hashExpressions;
	private ThetaJoinerDynamicAdvisedEpochsNew _joiner;
	private ThetaReshufflerAdvisedEpochsNew _reshuffler;
	private final ChainOperator _chain = new ChainOperator();
	private boolean _printOut;
	private boolean _printOutSet; // whether printOut was already set
	private Predicate _joinPredicate;
	private int _joinerParallelism;
	private InterchangingComponent _interComp = null;

	public ThetaJoinDynamicComponentAdvisedEpochs(Component firstParent, Component secondParent,
			QueryPlan queryPlan) {
		_firstParent = firstParent;
		_firstParent.setChild(this);
		_secondParent = secondParent;
		if (_secondParent != null) {
			_secondParent.setChild(this);
			_componentName = firstParent.getName() + "_" + secondParent.getName();
		} else
			_componentName = firstParent.getName().split("-")[0] + "_"
					+ firstParent.getName().split("-")[1];
		queryPlan.add(this);
	}

	@Override
	public ThetaJoinDynamicComponentAdvisedEpochs addOperator(Operator operator) {
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
		throw new RuntimeException("Load balancing for Dynamic Theta join is done inherently!");
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
		hash = 37 * hash + (_componentName != null ? _componentName.hashCode() : 0);
		return hash;
	}

	@Override
	public void makeBolts(TopologyBuilder builder, TopologyKiller killer,
			List<String> allCompNames, Config conf, int partitioningType, int hierarchyPosition) {

		// by default print out for the last component
		// for other conditions, can be set via setPrintOut
		if (hierarchyPosition == StormComponent.FINAL_COMPONENT && !_printOutSet)
			setPrintOut(true);
		_joinerParallelism = SystemParameters.getInt(conf, _componentName + "_PAR");
		MyUtilities.checkBatchOutput(_batchOutputMillis, _chain.getAggregation(), conf);

		int firstCardinality, secondCardinality;
		if (_secondParent == null) { // then first has to be of type
			// Interchanging Emitter
			firstCardinality = SystemParameters.getInt(conf, _firstParent.getName().split("-")[0]
					+ "_CARD");
			secondCardinality = SystemParameters.getInt(conf, _firstParent.getName().split("-")[1]
					+ "_CARD");
		} else {
			firstCardinality = SystemParameters.getInt(conf, _firstParent.getName() + "_CARD");
			secondCardinality = SystemParameters.getInt(conf, _secondParent.getName() + "_CARD");
		}

		final EquiMatrixAssignment _currentMappingAssignment = new EquiMatrixAssignment(
				firstCardinality, secondCardinality, _joinerParallelism, -1);

		final String dim = _currentMappingAssignment.getMappingDimensions();
		// dim ="1-1"; //initiate Splitting
		LOG.info(_componentName + "Initial Dimensions is: " + dim);

		// create the bolts ..

		// Create the reshuffler.
		_reshuffler = new ThetaReshufflerAdvisedEpochsNew(_firstParent, _secondParent,
				allCompNames, _joinerParallelism, hierarchyPosition, conf, builder, dim);

		if (_interComp != null)
			_reshuffler.set_interComp(_interComp);

		// Create the Join Bolt.
		_joiner = new ThetaJoinerDynamicAdvisedEpochsNew(_firstParent, _secondParent, this,
				allCompNames, _joinPredicate, hierarchyPosition, builder, killer, conf,
				_reshuffler, dim);
		_reshuffler.setJoinerID(_joiner.getID());

		/*
		 * setup communication between the components.
		 */
		// 1) Hook up the mapper&Synchronizer(reshuffler) to the reshuffler
		_reshuffler.getCurrentBolt().directGrouping(_reshuffler.getID(),
				SystemParameters.ThetaSynchronizerSignal);

		// 2) Hook up the previous emitters to the reshuffler

		final ThetaJoinDynamicMapping dMap = new ThetaJoinDynamicMapping(conf, -1);
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
	public ThetaJoinDynamicComponentAdvisedEpochs setBatchOutputMillis(long millis) {
		_batchOutputMillis = millis;
		return this;
	}

	@Override
	public void setChild(Component child) {
		_child = child;
	}

	// list of distinct keys, used for direct stream grouping and load-balancing
	// ()
	@Override
	public ThetaJoinStaticComponent setFullHashList(List<String> fullHashList) {
		throw new RuntimeException("Load balancing for Dynamic Theta join is done inherently!");
	}

	@Override
	public ThetaJoinDynamicComponentAdvisedEpochs setHashExpressions(
			List<ValueExpression> hashExpressions) {
		_hashExpressions = hashExpressions;
		return this;
	}

	@Override
	public ThetaJoinDynamicComponentAdvisedEpochs setHashIndexes(List<Integer> hashIndexes) {
		_hashIndexes = hashIndexes;
		return this;
	}

	public ThetaJoinDynamicComponentAdvisedEpochs setInterComp(InterchangingComponent _interComp) {
		this._interComp = _interComp;
		return this;
	}

	public Component setJoinPredicate(Predicate joinPredicate) {
		_joinPredicate = joinPredicate;
		return this;
	}

	@Override
	public ThetaJoinDynamicComponentAdvisedEpochs setPrintOut(boolean printOut) {
		_printOutSet = true;
		_printOut = printOut;
		return this;
	}

}
