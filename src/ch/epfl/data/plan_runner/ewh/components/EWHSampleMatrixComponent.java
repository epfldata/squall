package ch.epfl.data.plan_runner.ewh.components;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.conversion.NumericConversion;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.ewh.storm_components.D2CombinerBolt;
import ch.epfl.data.plan_runner.ewh.storm_components.EWHSampleMatrixBolt;
import ch.epfl.data.plan_runner.ewh.storm_components.OkcanSampleMatrixBolt;
import ch.epfl.data.plan_runner.ewh.storm_components.S1ReservoirGenerator;
import ch.epfl.data.plan_runner.ewh.storm_components.S1ReservoirMerge;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.operators.Operator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.predicates.Predicate;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import ch.epfl.data.plan_runner.storage.AggregationStorage;
import ch.epfl.data.plan_runner.storage.BasicStore;
import ch.epfl.data.plan_runner.storage.KeyValueStore;
import ch.epfl.data.plan_runner.storm_components.InterchangingComponent;
import ch.epfl.data.plan_runner.storm_components.StormComponent;
import ch.epfl.data.plan_runner.storm_components.StormDstJoin;
import ch.epfl.data.plan_runner.storm_components.StormDstTupleStorageBDB;
import ch.epfl.data.plan_runner.storm_components.StormDstTupleStorageJoin;
import ch.epfl.data.plan_runner.storm_components.StormEmitter;
import ch.epfl.data.plan_runner.storm_components.StormSrcJoin;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.SystemParameters;
import ch.epfl.data.plan_runner.utilities.SystemParameters.HistogramType;
import backtype.storm.Config;
import backtype.storm.task.IBolt;
import backtype.storm.topology.TopologyBuilder;

public class EWHSampleMatrixComponent implements Component {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(EWHSampleMatrixComponent.class);

	// there might be multiple children, but we actually inquiry only if it's null or not
	private Component _firstParent, _secondParent, _child;
	private String _componentName;
	
	private boolean _isFirstD2;
	private int _numOfLastJoiners;
	private ComparisonPredicate _comparison;
	private NumericConversion _wrapper;
	private int _firstRelationSize, _secondRelationSize;
	private int _firstNumOfBuckets, _secondNumOfBuckets;
	
	public EWHSampleMatrixComponent(Component firstParent, Component secondParent, boolean isFirstD2,
			NumericConversion keyType, ComparisonPredicate comparison, int numOfLastJoiners, 
			int firstRelationSize, int secondRelationSize,
			int firstNumOfBuckets, int secondNumOfBuckets) {
		_firstParent = firstParent;
		_firstParent.setChild(this);
		_secondParent = secondParent;
		_secondParent.setChild(this);
		_componentName = "EWH_SAMPLE_";
		
		_isFirstD2 = isFirstD2;
		_numOfLastJoiners = numOfLastJoiners;
		_comparison = comparison;
		_wrapper = keyType;
		_firstRelationSize = firstRelationSize;
		_secondRelationSize = secondRelationSize;
		_firstNumOfBuckets = firstNumOfBuckets;
		_secondNumOfBuckets = secondNumOfBuckets;
	}
	
	@Override
	public String getName() {
		return _componentName;
	}	

	@Override
	public void setChild(Component child) {
		_child = child;
	}

	@Override
	public Component getChild() {
		return _child;
	}

	@Override
	public Component[] getParents() {
		return new Component[] { _firstParent, _secondParent };
	}
	
	@Override
	public List<DataSourceComponent> getAncestorDataSources() {
		final List<DataSourceComponent> list = new ArrayList<DataSourceComponent>();
		for (final Component parent : getParents())
			list.addAll(parent.getAncestorDataSources());
		return list;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Component)
			return _componentName.equals(((Component) obj).getName());
		else
			return false;
	}	
	
	@Override
	public int hashCode() {
		int hash = 7;
		hash = 37 * hash + (_componentName != null ? _componentName.hashCode() : 0);
		return hash;
	}

	@Override
	public void makeBolts(TopologyBuilder builder, TopologyKiller killer,
			List<String> allCompNames, Config conf, int hierarchyPosition) {
	
		Component d2Source = null;
		Component s1Source = null;
		if (_isFirstD2){
			d2Source = _firstParent;
			s1Source = _secondParent;
		}else{
			d2Source = _secondParent;
			s1Source = _firstParent;
		}
		
		// isEWHS1Histogram = true means D2Combiner computes only d2_equi and S1Reservoir computes d2
		boolean isEWHS1Histogram = SystemParameters.getBooleanIfExist(conf, HistogramType.S1_RES_HIST.readConfEntryName());
		// we need to add these StormEmitters to allCompNames in order to have emitterIndex different than -1
		String d2CombinerName = _componentName + "D2_COMBINER";
		String s1ReservoirGeneratorName = _componentName + "S1_RESERVOIR";
		String s1ReservoirMergeName = _componentName + "S1_RESERVOIR_MERGE";
		String partitionerName = _componentName + "PARTITIONER";
		allCompNames.addAll(Arrays.asList(d2CombinerName, s1ReservoirGeneratorName, s1ReservoirMergeName, partitionerName));
		
		// hierarchyPosition of all but last bolt are StormComponent.INTERMEDIATE
		D2CombinerBolt d2Combiner = new D2CombinerBolt(d2Source, s1ReservoirMergeName, d2CombinerName, _isFirstD2, _wrapper, _comparison, isEWHS1Histogram,
				_firstNumOfBuckets, _secondNumOfBuckets,
				allCompNames, StormComponent.INTERMEDIATE, builder, killer, conf);
		
		S1ReservoirGenerator s1ReservoirGenerator = new S1ReservoirGenerator(d2Combiner, s1Source, s1ReservoirGeneratorName, partitionerName, 
				isEWHS1Histogram,
				_wrapper, _comparison, _firstNumOfBuckets, _secondNumOfBuckets,
				allCompNames, StormComponent.INTERMEDIATE, builder, killer, conf);

		S1ReservoirMerge s1ReservoirMerge = new S1ReservoirMerge(s1ReservoirGenerator, s1ReservoirMergeName,
				_wrapper, _comparison, _firstNumOfBuckets, _secondNumOfBuckets,
				allCompNames, StormComponent.INTERMEDIATE, builder, killer, conf);
		
		EWHSampleMatrixBolt partitioner = new EWHSampleMatrixBolt(_firstParent, _secondParent, d2Combiner, s1ReservoirGenerator, partitionerName, _numOfLastJoiners, 
				_firstRelationSize, _secondRelationSize,
				_wrapper, _comparison, _firstNumOfBuckets, _secondNumOfBuckets, allCompNames, builder, killer, conf);
	}

	// below is not used
	@Override
	public EWHSampleMatrixComponent add(Operator operator) {
		throw new RuntimeException("Should not be here!");
	}
	
	@Override
	public long getBatchOutputMillis() {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public ChainOperator getChainOperator() {
		throw new RuntimeException("Should not be here!");
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
	public boolean getPrintOut() {
		throw new RuntimeException("Should not be here!");
	}
	
	
	@Override
	public EWHSampleMatrixComponent setBatchOutputMillis(long millis) {
		throw new RuntimeException("Should not be here!");
	}

	// list of distinct keys, used for direct stream grouping and load-balancing
	// ()
	@Override
	public EWHSampleMatrixComponent setFullHashList(List<String> fullHashList) {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public EWHSampleMatrixComponent setHashExpressions(List<ValueExpression> hashExpressions) {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public EWHSampleMatrixComponent setOutputPartKey(List<Integer> hashIndexes) {
		throw new RuntimeException("Should not be here!");
	}
	
	@Override
	public EWHSampleMatrixComponent setOutputPartKey(int... hashIndexes) {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public EWHSampleMatrixComponent setPrintOut(boolean printOut) {
		throw new RuntimeException("Should not be here!");
	}

	// Out of the second storage (join of R tuple with S relation)
	public EWHSampleMatrixComponent setSecondPreAggProj(ProjectOperator secondPreAggProj) {
		throw new RuntimeException("Should not be here!");
	}

	public EWHSampleMatrixComponent setSecondPreAggStorage(AggregationStorage secondPreAggStorage) {
		throw new RuntimeException("Should not be here!");
	}
	
	@Override
	public Component setInterComp(InterchangingComponent inter) {
		throw new RuntimeException("EquiJoin component does not support setInterComp");
	}
	
	@Override
	public EWHSampleMatrixComponent setJoinPredicate(Predicate predicate) {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public Component setContentSensitiveThetaJoinWrapper(TypeConversion wrapper) {
		throw new RuntimeException("Should not be here!");
	}
}