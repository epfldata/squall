package ch.epfl.data.plan_runner.ewh.components;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.components.EquiJoinComponent;
import ch.epfl.data.plan_runner.conversion.NumericConversion;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.ewh.storm_components.CreateHistogramBolt;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.operators.Operator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.predicates.Predicate;
import ch.epfl.data.plan_runner.storage.AggregationStorage;
import ch.epfl.data.plan_runner.storm_components.InterchangingComponent;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;

// equi-depth histogram on one or both input relations
public class CreateHistogramComponent implements Component {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(EquiJoinComponent.class);

	private final Component _r1, _r2;
	private List<Component> _parents = new ArrayList<Component>();
	private String _componentName;

	private int _numOfLastJoiners;
	private ComparisonPredicate _comparison;
	private NumericConversion _wrapper;

	public CreateHistogramComponent(Component r1, Component r2,
			NumericConversion keyType, ComparisonPredicate comparison,
			int numOfLastJoiners) {
		_r1 = r1;
		_r2 = r2;
		if (_r1 != null) {
			_r1.setChild(this);
			_parents.add(_r1);
		}
		if (_r2 != null) {
			_r2.setChild(this);
			_parents.add(_r2);
		}
		_componentName = "SRC_HISTOGRAM";

		_numOfLastJoiners = numOfLastJoiners;
		_comparison = comparison;
		_wrapper = keyType;
	}

	// below is not used
	@Override
	public CreateHistogramComponent add(Operator operator) {
		throw new RuntimeException("Should not be here!");
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
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public ChainOperator getChainOperator() {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public Component getChild() {
		return null;
	}

	// from StormEmitter interface
	@Override
	public String[] getEmitterIDs() {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public List<String> getFullHashList() {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public List<ValueExpression> getHashExpressions() {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public List<Integer> getHashIndexes() {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public String getInfoID() {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public String getName() {
		return _componentName;
	}

	@Override
	public Component[] getParents() {
		Component[] parentsArr = new Component[_parents.size()];
		for (int i = 0; i < _parents.size(); i++) {
			parentsArr[i] = _parents.get(i);
		}
		return parentsArr;
	}

	@Override
	public boolean getPrintOut() {
		throw new RuntimeException("Should not be here!");
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

		new CreateHistogramBolt(_r1, _r2, _componentName, _numOfLastJoiners,
				_wrapper, _comparison, allCompNames, builder, killer, conf);
	}

	@Override
	public CreateHistogramComponent setBatchOutputMillis(long millis) {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public void setChild(Component child) {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public Component setContentSensitiveThetaJoinWrapper(TypeConversion wrapper) {
		throw new RuntimeException("Should not be here!");
	}

	// list of distinct keys, used for direct stream grouping and load-balancing
	// ()
	@Override
	public CreateHistogramComponent setFullHashList(List<String> fullHashList) {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public CreateHistogramComponent setHashExpressions(
			List<ValueExpression> hashExpressions) {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public Component setInterComp(InterchangingComponent inter) {
		throw new RuntimeException(
				"EquiJoin component does not support setInterComp");
	}

	@Override
	public CreateHistogramComponent setJoinPredicate(Predicate predicate) {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public CreateHistogramComponent setOutputPartKey(int... hashIndexes) {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public CreateHistogramComponent setOutputPartKey(List<Integer> hashIndexes) {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public CreateHistogramComponent setPrintOut(boolean printOut) {
		throw new RuntimeException("Should not be here!");
	}

	// Out of the second storage (join of R tuple with S relation)
	public CreateHistogramComponent setSecondPreAggProj(
			ProjectOperator secondPreAggProj) {
		throw new RuntimeException("Should not be here!");
	}

	public CreateHistogramComponent setSecondPreAggStorage(
			AggregationStorage secondPreAggStorage) {
		throw new RuntimeException("Should not be here!");
	}
}