package plan_runner.storm_components;

import java.util.ArrayList;
import java.util.List;

import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.Operator;
import plan_runner.query_plans.QueryPlan;
import plan_runner.storm_components.synchronization.TopologyKiller;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class InterchangingComponent implements Component {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final Component _firstParent;
	private final Component _secondParent;
	private Component _child;
	private final String _componentName;
	private long _batchOutputMillis;
	private List<Integer> _hashIndexes;
	private List<ValueExpression> _hashExpressions;

	private InterchangingBolt _interBolt;
	private boolean _printOut;
	private final int _multFactor;

	public InterchangingComponent(Component firstParent, Component secondParent,
			QueryPlan queryPlan, int multfactor) {
		_firstParent = firstParent;
		_firstParent.setChild(this);
		_secondParent = secondParent;
		_secondParent.setChild(this);
		_componentName = firstParent.getName() + "_" + secondParent.getName() + "_INTER";
		queryPlan.add(this);
		_multFactor = multfactor;
	}

	@Override
	public Component addOperator(Operator operator) {
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
		return null;
	}

	@Override
	public Component getChild() {
		return _child;
	}

	// from StormEmitter interface
	@Override
	public String[] getEmitterIDs() {
		return new String[] { _componentName };
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
		return _interBolt.getInfoID();
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

		_interBolt = new InterchangingBolt(_firstParent, _secondParent, this, allCompNames,
				builder, killer, conf, _multFactor);

	}

	@Override
	public Component setBatchOutputMillis(long millis) {
		_batchOutputMillis = millis;
		return this;
	}

	@Override
	public void setChild(Component child) {
		_child = child;
	}

	@Override
	public Component setFullHashList(List<String> fullHashList) {
		throw new RuntimeException("Load balancing for Dynamic Theta join is done inherently!");
	}

	@Override
	public Component setHashExpressions(List<ValueExpression> hashExpressions) {
		_hashExpressions = hashExpressions;
		return this;
	}

	@Override
	public Component setHashIndexes(List<Integer> hashIndexes) {
		_hashIndexes = hashIndexes;
		return this;
	}

	@Override
	public Component setPrintOut(boolean printOut) {
		_printOut = printOut;
		return this;
	}
}
