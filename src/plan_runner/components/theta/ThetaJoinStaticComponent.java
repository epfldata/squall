package plan_runner.components.theta;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.Operator;
import plan_runner.predicates.Predicate;
import plan_runner.query_plans.QueryPlan;
import plan_runner.storm_components.InterchangingComponent;
import plan_runner.storm_components.StormBoltComponent;
import plan_runner.storm_components.StormComponent;
import plan_runner.storm_components.synchronization.TopologyKiller;
import plan_runner.storm_components.theta.StormThetaJoin;
import plan_runner.storm_components.theta.StormThetaJoinBDB;
import plan_runner.utilities.MyUtilities;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class ThetaJoinStaticComponent implements Component {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(ThetaJoinStaticComponent.class);
	private final Component _firstParent;
	private final Component _secondParent;
	private Component _child;
	private final String _componentName;
	private long _batchOutputMillis;
	private List<Integer> _hashIndexes;
	private List<ValueExpression> _hashExpressions;
	private StormBoltComponent _joiner;
	private final ChainOperator _chain = new ChainOperator();
	private boolean _printOut;
	private boolean _printOutSet; // whether printOut was already set
	private boolean _isContentSensitive;
	private Predicate _joinPredicate;
	private InterchangingComponent _interComp = null;
	private TypeConversion _contentSensitiveThetaJoinWrapper=null; 

	//equi-weight histogram
	private boolean _isPartitioner; 
	
	public ThetaJoinStaticComponent(Component firstParent, Component secondParent,
			QueryPlan queryPlan, boolean isContentSensitive) {
		_firstParent = firstParent;
		_firstParent.setChild(this);
		_secondParent = secondParent;
		_secondParent.setChild(this);
		_componentName = firstParent.getName() + "_" + secondParent.getName();
		_isContentSensitive=isContentSensitive;

		queryPlan.add(this);
	}

	@Override
	public ThetaJoinStaticComponent addOperator(Operator operator) {
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
		throw new RuntimeException("Load balancing for Theta join is done inherently!");
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

		MyUtilities.checkBatchOutput(_batchOutputMillis, _chain.getAggregation(), conf);

		boolean isBDB = MyUtilities.isBDB(conf);
		if(isBDB && _joinPredicate == null){
			throw new RuntimeException("Please provide _joinPredicate if you want to run BDB!");
		}
		
		if(isBDB && (hierarchyPosition == StormComponent.FINAL_COMPONENT)){
			_joiner = new StormThetaJoinBDB(_firstParent, _secondParent, this, allCompNames,
					_joinPredicate, hierarchyPosition, builder, killer, conf, _interComp);
		}else{
			_joiner = new StormThetaJoin(_firstParent, _secondParent, this, allCompNames,
					_joinPredicate, _isPartitioner, hierarchyPosition, builder, killer, conf, 
					_interComp, _isContentSensitive,_contentSensitiveThetaJoinWrapper);
		}
	}

	@Override
	public ThetaJoinStaticComponent setBatchOutputMillis(long millis) {
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
		throw new RuntimeException("Load balancing for Theta join is done inherently!");
	}

	@Override
	public ThetaJoinStaticComponent setHashExpressions(List<ValueExpression> hashExpressions) {
		_hashExpressions = hashExpressions;
		return this;
	}

	@Override
	public ThetaJoinStaticComponent setHashIndexes(List<Integer> hashIndexes) {
		_hashIndexes = hashIndexes;
		return this;
	}

	@Override
	public ThetaJoinStaticComponent setInterComp(InterchangingComponent inter) {
		_interComp = inter;
		return this;
	}

	@Override
	public ThetaJoinStaticComponent setJoinPredicate(Predicate joinPredicate) {
		_joinPredicate = joinPredicate;
		return this;
	}

	@Override
	public ThetaJoinStaticComponent setPrintOut(boolean printOut) {
		_printOutSet = true;
		_printOut = printOut;
		return this;
	}

	@Override
	public ThetaJoinStaticComponent setContentSensitiveThetaJoinWrapper(TypeConversion wrapper) {
		_contentSensitiveThetaJoinWrapper=wrapper;
		return this;
	}
	
	public ThetaJoinStaticComponent setPartitioner(boolean isPartitioner){
		_isPartitioner = isPartitioner;
		return this;
	}	

}