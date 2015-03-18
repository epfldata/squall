package ch.epfl.data.plan_runner.components;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.operators.Operator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.predicates.Predicate;
import ch.epfl.data.plan_runner.storage.AggregationStorage;
import ch.epfl.data.plan_runner.storage.BasicStore;
import ch.epfl.data.plan_runner.storage.KeyValueStore;
import ch.epfl.data.plan_runner.storm_components.InterchangingComponent;
import ch.epfl.data.plan_runner.storm_components.StormComponent;
import ch.epfl.data.plan_runner.storm_components.StormDstJoin;
import ch.epfl.data.plan_runner.storm_components.StormDstTupleStorageBDB;
import ch.epfl.data.plan_runner.storm_components.StormDstTupleStorageJoin;
import ch.epfl.data.plan_runner.storm_components.StormEmitter;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.window_semantics.WindowSemanticsManager;

public class EquiJoinComponent extends JoinerComponent implements Component {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(EquiJoinComponent.class);

	private final Component _firstParent;
	private final Component _secondParent;
	private Component _child;

	private final String _componentName;

	private long _batchOutputMillis;

	private List<Integer> _hashIndexes;
	private List<ValueExpression> _hashExpressions;

	private StormEmitter _joiner;

	private final ChainOperator _chain = new ChainOperator();

	// The storage is actually KeyValue<String, String>
	// or AggregationStorage<Numeric> for pre-aggregation
	// Access method returns a list of Strings (a list of Numerics for
	// pre-aggregation)
	private BasicStore<ArrayList<String>> _firstStorage, _secondStorage;
	// preAggregation
	private ProjectOperator _firstPreAggProj, _secondPreAggProj;

	private boolean _printOut;
	private boolean _printOutSet; // whether printOut was already set

	private List<String> _fullHashList;
	private Predicate _joinPredicate;

	private boolean _isRemoveIndex = true;

	public EquiJoinComponent(Component firstParent, Component secondParent) {
		_firstParent = firstParent;
		_firstParent.setChild(this);
		_secondParent = secondParent;
		_secondParent.setChild(this);

		_componentName = firstParent.getName() + "_" + secondParent.getName();
	}

	public EquiJoinComponent(Component firstParent, Component secondParent,
			boolean isRemoveIndex) {
		_firstParent = firstParent;
		_firstParent.setChild(this);
		_secondParent = secondParent;
		_secondParent.setChild(this);
		_componentName = firstParent.getName() + "_" + secondParent.getName();
		_isRemoveIndex = isRemoveIndex;
	}

	@Override
	public EquiJoinComponent add(Operator operator) {
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
		return _fullHashList;
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

		MyUtilities.checkBatchOutput(_batchOutputMillis,
				_chain.getAggregation(), conf);

		// If not set in Preaggregation, we set normal storages
		if (_firstStorage == null)
			_firstStorage = new KeyValueStore<String, String>(conf);
		if (_secondStorage == null)
			_secondStorage = new KeyValueStore<String, String>(conf);

		boolean isBDB = MyUtilities.isBDB(conf);
		if (isBDB && _joinPredicate == null) {
			throw new RuntimeException(
					"Please provide _joinPredicate if you want to run BDB!");
		}

		// TODO: what is with the if condition
		if (isBDB && (hierarchyPosition == StormComponent.FINAL_COMPONENT)) {
			_joiner = new StormDstTupleStorageBDB(_firstParent, _secondParent,
					this, allCompNames, _joinPredicate, hierarchyPosition,
					builder, killer, conf);
		} else if (_joinPredicate != null) {
			_joiner = new StormDstTupleStorageJoin(_firstParent, _secondParent,
					this, allCompNames, _joinPredicate, hierarchyPosition,
					builder, killer, conf);
		} else {
			// should issue a warning
			_joiner = new StormDstJoin(_firstParent, _secondParent, this,
					allCompNames, _firstStorage, _secondStorage,
					_firstPreAggProj, _secondPreAggProj, hierarchyPosition,
					builder, killer, conf, _isRemoveIndex);
		}
	}

	@Override
	public EquiJoinComponent setBatchOutputMillis(long millis) {
		_batchOutputMillis = millis;
		return this;
	}

	@Override
	public void setChild(Component child) {
		_child = child;
	}

	@Override
	public Component setContentSensitiveThetaJoinWrapper(TypeConversion wrapper) {
		return this;
	}

	// Out of the first storage (join of S tuple with R relation)
	public EquiJoinComponent setFirstPreAggProj(ProjectOperator firstPreAggProj) {
		_firstPreAggProj = firstPreAggProj;
		return this;
	}

	// next four methods are for Preaggregation
	public EquiJoinComponent setFirstPreAggStorage(
			AggregationStorage firstPreAggStorage) {
		_firstStorage = firstPreAggStorage;
		return this;
	}

	// list of distinct keys, used for direct stream grouping and load-balancing
	// ()
	@Override
	public EquiJoinComponent setFullHashList(List<String> fullHashList) {
		_fullHashList = fullHashList;
		return this;
	}

	@Override
	public EquiJoinComponent setHashExpressions(
			List<ValueExpression> hashExpressions) {
		_hashExpressions = hashExpressions;
		return this;
	}

	@Override
	public Component setInterComp(InterchangingComponent inter) {
		throw new RuntimeException(
				"EquiJoin component does not support setInterComp");
	}

	@Override
	public EquiJoinComponent setJoinPredicate(Predicate predicate) {
		_joinPredicate = predicate;
		return this;
	}

	@Override
	public EquiJoinComponent setOutputPartKey(int... hashIndexes) {
		return setOutputPartKey(Arrays.asList(ArrayUtils.toObject(hashIndexes)));
	}

	@Override
	public EquiJoinComponent setOutputPartKey(List<Integer> hashIndexes) {
		_hashIndexes = hashIndexes;
		return this;
	}

	@Override
	public EquiJoinComponent setPrintOut(boolean printOut) {
		_printOutSet = true;
		_printOut = printOut;
		return this;
	}

	// Out of the second storage (join of R tuple with S relation)
	public EquiJoinComponent setSecondPreAggProj(
			ProjectOperator secondPreAggProj) {
		_secondPreAggProj = secondPreAggProj;
		return this;
	}

	public EquiJoinComponent setSecondPreAggStorage(
			AggregationStorage secondPreAggStorage) {
		_secondStorage = secondPreAggStorage;
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
		return this;
	}

}
