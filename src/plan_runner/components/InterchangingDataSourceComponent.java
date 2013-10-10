package plan_runner.components;

import java.util.List;

import org.apache.log4j.Logger;

import plan_runner.expressions.ValueExpression;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.Operator;
import plan_runner.query_plans.QueryPlan;
import plan_runner.storm_components.StormComponent;
import plan_runner.storm_components.StormInterchangingDataSource;
import plan_runner.storm_components.synchronization.TopologyKiller;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class InterchangingDataSourceComponent implements Component {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(InterchangingDataSourceComponent.class);

	private final String _componentName;
	private final String _inputPathRel1, _inputPathRel2;

	private long _batchOutputMillis;

	private List<Integer> _hashIndexes;
	private List<ValueExpression> _hashExpressions;

	private StormInterchangingDataSource _dataSource;

	private final ChainOperator _chainRel1 = new ChainOperator();
	private final ChainOperator _chainRel2 = new ChainOperator();

	private boolean _printOut;
	private boolean _printOutSet; // whether printOut condition is already set

	private Component _child;

	private final int _multFactor;

	public InterchangingDataSourceComponent(String componentName, String inputPath1,
			String inputPath2, QueryPlan queryPlan, int multfactor) {
		_componentName = componentName;
		_inputPathRel1 = inputPath1;
		_inputPathRel2 = inputPath2;
		_multFactor = multfactor;
		queryPlan.add(this);
	}

	@Override
	public Component addOperator(Operator operator) {
		return null;
	}

	public InterchangingDataSourceComponent addOperatorRel1(Operator operator) {
		_chainRel1.addOperator(operator);
		return this;
	}

	public InterchangingDataSourceComponent addOperatorRel2(Operator operator) {
		_chainRel2.addOperator(operator);
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
		// List<InterchangingDataSourceComponent> list = new
		// ArrayList<InterchangingDataSourceComponent>();
		// list.add(this);
		// return list;
		return null;
	}

	@Override
	public long getBatchOutputMillis() {
		return _batchOutputMillis;
	}

	// IGNORE
	@Override
	public ChainOperator getChainOperator() {
		return null;
	}

	public ChainOperator getChainOperatorRel1() {
		return _chainRel1;
	}

	public ChainOperator getChainOperatorRel2() {
		return _chainRel2;
	}

	@Override
	public Component getChild() {
		return _child;
	}

	// from StormEmitter interface
	@Override
	public String[] getEmitterIDs() {
		return _dataSource.getEmitterIDs();
	}

	@Override
	public List<String> getFullHashList() {
		throw new RuntimeException("This method should not be invoked for DataSourceComponent!");
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
		return _dataSource.getInfoID() + "\n";
	}

	@Override
	public String getName() {
		return _componentName;
	}

	@Override
	public Component[] getParents() {
		return null;
	}

	@Override
	public boolean getPrintOut() {
		return _printOut;
	}

	@Override
	public int hashCode() {
		int hash = 3;
		hash = 59 * hash + (_componentName != null ? _componentName.hashCode() : 0);
		return hash;
	}

	@Override
	public void makeBolts(TopologyBuilder builder, TopologyKiller killer,
			List<String> allCompNames, Config conf, int partitioningType, int hierarchyPosition) {

		// by default print out for the last component
		// for other conditions, can be set via setPrintOut
		if (hierarchyPosition == StormComponent.FINAL_COMPONENT && !_printOutSet)
			setPrintOut(true);

		_dataSource = new StormInterchangingDataSource(this, allCompNames, _multFactor,
				_inputPathRel1, _inputPathRel2, hierarchyPosition, builder, killer, conf);
	}

	@Override
	public InterchangingDataSourceComponent setBatchOutputMillis(long millis) {
		throw new RuntimeException("Setting batch mode is not allowed for DataSourceComponents!");
		// _batchOutputMillis = millis;
		// return this;
	}

	@Override
	public void setChild(Component child) {
		_child = child;
	}

	@Override
	public InterchangingDataSourceComponent setFullHashList(List<String> fullHashList) {
		throw new RuntimeException("This method should not be invoked for DataSourceComponent!");
	}

	@Override
	public InterchangingDataSourceComponent setHashExpressions(List<ValueExpression> hashExpressions) {
		_hashExpressions = hashExpressions;
		return this;
	}

	@Override
	public InterchangingDataSourceComponent setHashIndexes(List<Integer> hashIndexes) {
		_hashIndexes = hashIndexes;
		return this;
	}

	@Override
	public InterchangingDataSourceComponent setPrintOut(boolean printOut) {
		_printOutSet = true;
		_printOut = printOut;
		return this;
	}

}