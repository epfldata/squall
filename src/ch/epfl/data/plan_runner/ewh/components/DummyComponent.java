package ch.epfl.data.plan_runner.ewh.components;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.ewh.storm_components.DummyBolt;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.operators.Operator;
import ch.epfl.data.plan_runner.predicates.Predicate;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import ch.epfl.data.plan_runner.storm_components.InterchangingComponent;
import ch.epfl.data.plan_runner.storm_components.StormComponent;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

public class DummyComponent implements Component {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(DummyComponent.class);

	private final String _componentName;

	private long _batchOutputMillis;

	private List<Integer> _hashIndexes;
	private List<ValueExpression> _hashExpressions;

	private final ChainOperator _chain = new ChainOperator();

	private boolean _printOut;
	private boolean _printOutSet;

	private final Component _parent;
	private Component _child;
	private DummyBolt _dummyBolt;

	private List<String> _fullHashList;

	public DummyComponent(Component parent, String componentName) {
		_parent = parent;
		_parent.setChild(this);

		_componentName = componentName;
	}

	@Override
	public DummyComponent add(Operator operator) {
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
		list.addAll(_parent.getAncestorDataSources());
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

	// from StormComponent
	@Override
	public String[] getEmitterIDs() {
		return _dummyBolt.getEmitterIDs();
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
		return _dummyBolt.getInfoID();
	}

	@Override
	public String getName() {
		return _componentName;
	}

	@Override
	public Component[] getParents() {
		return new Component[] { _parent };
	}

	@Override
	public boolean getPrintOut() {
		return _printOut;
	}

	@Override
	public int hashCode() {
		int hash = 5;
		hash = 47 * hash + (_componentName != null ? _componentName.hashCode() : 0);
		return hash;
	}

	@Override
	public void makeBolts(TopologyBuilder builder, TopologyKiller killer,
			List<String> allCompNames, Config conf, int hierarchyPosition) {

		// by default print out for the last component
		// for other conditions, can be set via setPrintOut
		if (hierarchyPosition == StormComponent.FINAL_COMPONENT && !_printOutSet)
			setPrintOut(true);

		MyUtilities.checkBatchOutput(_batchOutputMillis, _chain.getAggregation(), conf);

		_dummyBolt = new DummyBolt(_parent, this, allCompNames, hierarchyPosition, builder,
				killer, conf);
	}

	@Override
	public DummyComponent setBatchOutputMillis(long millis) {
		_batchOutputMillis = millis;
		return this;
	}

	@Override
	public void setChild(Component child) {
		_child = child;
	}

	@Override
	public DummyComponent setFullHashList(List<String> fullHashList) {
		_fullHashList = fullHashList;
		return this;
	}

	@Override
	public DummyComponent setHashExpressions(List<ValueExpression> hashExpressions) {
		_hashExpressions = hashExpressions;
		return this;
	}

	@Override
	public DummyComponent setOutputPartKey(List<Integer> hashIndexes) {
		_hashIndexes = hashIndexes;
		return this;
	}
	
	@Override
	public DummyComponent setOutputPartKey(int... hashIndexes) {
		_hashIndexes = Arrays.asList(ArrayUtils.toObject(hashIndexes));
		return this;
	}

	@Override
	public DummyComponent setPrintOut(boolean printOut) {
		_printOutSet = true;
		_printOut = printOut;
		return this;
	}
	
	@Override
	public Component setInterComp(InterchangingComponent inter) {
		throw new RuntimeException("Operator component does not support setInterComp");
	}

	@Override
	public Component setJoinPredicate(Predicate joinPredicate) {
		throw new RuntimeException("Operator component does not support Join Predicates");
	}

	@Override
	public Component setContentSensitiveThetaJoinWrapper(TypeConversion wrapper) {
		return this;
	}

}