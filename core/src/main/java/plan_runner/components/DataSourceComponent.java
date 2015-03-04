package plan_runner.components;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import plan_runner.conversion.TypeConversion;
import plan_runner.ewh.components.CreateHistogramComponent;
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.Operator;
import plan_runner.predicates.Predicate;
import plan_runner.storm_components.InterchangingComponent;
import plan_runner.storm_components.StormComponent;
import plan_runner.storm_components.StormDataSource;
import plan_runner.storm_components.synchronization.TopologyKiller;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.SystemParameters;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class DataSourceComponent implements Component {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(DataSourceComponent.class);

	private final String _componentName;
	private final String _inputPath;

	private long _batchOutputMillis;

	private List<Integer> _hashIndexes;
	private List<ValueExpression> _hashExpressions;

	private StormDataSource _dataSource;

	private final ChainOperator _chain = new ChainOperator();

	private boolean _printOut;
	private boolean _printOutSet; // whether printOut condition is already set

	private Component _child;
	
	//equi-weight histogram
	private boolean _isPartitioner;

	public DataSourceComponent(String componentName, String inputPath) {
		_componentName = componentName;
		_inputPath = inputPath;
	}

	@Override
	public DataSourceComponent add(Operator operator) {
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
		list.add(this);
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

		final int parallelism = SystemParameters.getInt(conf, _componentName + "_PAR");
		if (parallelism > 1 && _chain.getDistinct() != null)
			throw new RuntimeException(
					_componentName
							+ ": Distinct operator cannot be specified for multiple spouts for one input file!");

		MyUtilities.checkBatchOutput(_batchOutputMillis, _chain.getAggregation(), conf);

		_dataSource = new StormDataSource(this, allCompNames, _inputPath, hierarchyPosition,
				parallelism, _isPartitioner, 
				builder, killer, conf);
	}

	@Override
	public DataSourceComponent setBatchOutputMillis(long millis) {
		throw new RuntimeException("Setting batch mode is not allowed for DataSourceComponents!");
		// _batchOutputMillis = millis;
		// return this;
	}

	@Override
	public void setChild(Component child) {
		_child = child;
	}

	@Override
	public DataSourceComponent setFullHashList(List<String> fullHashList) {
		throw new RuntimeException("This method should not be invoked for DataSourceComponent!");
	}

	@Override
	public DataSourceComponent setHashExpressions(List<ValueExpression> hashExpressions) {
		_hashExpressions = hashExpressions;
		return this;
	}

	@Override
	public DataSourceComponent setOutputPartKey(List<Integer> hashIndexes) {
		_hashIndexes = hashIndexes;
		return this;
	}
	
	@Override
	public DataSourceComponent setOutputPartKey(int... hashIndexes) {
		return setOutputPartKey(Arrays.asList(ArrayUtils.toObject(hashIndexes)));
	}

	@Override
	public DataSourceComponent setPrintOut(boolean printOut) {
		_printOutSet = true;
		_printOut = printOut;
		return this;
	}
	
	public DataSourceComponent setPartitioner(boolean isPartitioner){
		_isPartitioner = isPartitioner;
		return this;
	}

	@Override
	public DataSourceComponent setInterComp(InterchangingComponent inter) {
		throw new RuntimeException("Datasource component does not support setInterComp");
	}

	@Override
	public DataSourceComponent setJoinPredicate(Predicate joinPredicate) {
		throw new RuntimeException("Datasource component does not support Join Predicates");
	}

	@Override
	public DataSourceComponent setContentSensitiveThetaJoinWrapper(TypeConversion wrapper) {
		return this;
	}

}