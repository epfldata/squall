package plan_runner.query_plans.ewh;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.components.theta.ThetaJoinComponentFactory;
import plan_runner.components.theta.ThetaJoinDynamicComponentAdvisedEpochs;
import plan_runner.components.theta.ThetaJoinStaticComponent;
import plan_runner.conversion.DateConversion;
import plan_runner.conversion.DateIntegerConversion;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.ewh.components.CreateHistogramComponent;
import plan_runner.ewh.components.EWHSampleMatrixComponent;
import plan_runner.ewh.data_structures.NumOfBuckets;
import plan_runner.ewh.operators.SampleAsideAndForwardOperator;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateCountOperator;
import plan_runner.operators.PrintOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SampleOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.AndPredicate;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.query_plans.QueryBuilder;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.SystemParameters;

public class EWHSampleMatrixPlan {
	private static Logger LOG = Logger.getLogger(EWHSampleMatrixPlan.class);
	private QueryBuilder _queryBuilder = new QueryBuilder();

	public EWHSampleMatrixPlan(String dataPath, String extension, Map conf) {
		//can be extracted from the complete query plan
		String firstCompName = SystemParameters.getString(conf, "FIRST_COMP_NAME");
		String secondCompName = SystemParameters.getString(conf, "SECOND_COMP_NAME");
		int firstProjection = SystemParameters.getInt(conf, "FIRST_KEY_PROJECT");
		int secondProjection = SystemParameters.getInt(conf, "SECOND_KEY_PROJECT");		
		NumericConversion keyType;
		String keyTypeStr = SystemParameters.getString(conf, "KEY_TYPE_STR");
		if(keyTypeStr.equals("DATE_INTEGER")){
			keyType = new DateIntegerConversion();
		}else if(keyTypeStr.equals("INTEGER")){
			keyType = new IntegerConversion();
		}else{
			throw new RuntimeException("Unsupported type " + keyTypeStr);
		}
		ComparisonPredicate comparison;
		String comparisonStr = SystemParameters.getString(conf, "COMPARISON_TYPE");
		if(comparisonStr.equals("EQUAL")){
			comparison = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP);
		}else if(comparisonStr.equals("SYM_BAND_WITH_BOUNDS_OP")){
			int comparisonValue = SystemParameters.getInt(conf, "COMPARISON_VALUE");
			comparison = new ComparisonPredicate(ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP, comparisonValue, keyType);
		}else{
			throw new RuntimeException("Unsupported comparison " + comparisonStr);
		}
		
		// cannot be extracted from the complete query plan
		String firstSrcFile = SystemParameters.getString(conf, "FIRST_SRC_FILE");
		String secondSrcFile = SystemParameters.getString(conf, "SECOND_SRC_FILE");
		int firstRelSize = SystemParameters.getInt(conf, "FIRST_REL_SIZE");
		int secondRelSize = SystemParameters.getInt(conf, "SECOND_REL_SIZE");
		long totalOutputSize = SystemParameters.getLong(conf, "TOTAL_OUTPUT_SIZE");
		int numLastJoiners = SystemParameters.getInt(conf, "PAR_LAST_JOINERS");
		
		ProjectOperator projectionLineitem1 = new ProjectOperator(new int[] {firstProjection});
		ProjectOperator projectionLineitem2 = new ProjectOperator(new int[] {secondProjection});
		final List<Integer> hash = Arrays.asList(0); // not important, always 0
		
		//compute Number of buckets
		NumOfBuckets numOfBuckets = MyUtilities.computeSampleMatrixBuckets(firstRelSize, secondRelSize, numLastJoiners, conf);
		int firstNumOfBuckets = numOfBuckets.getXNumOfBuckets();
		int secondNumOfBuckets = numOfBuckets.getYNumOfBuckets();
		LOG.info("In the sample matrix, FirstNumOfBuckets = " + firstNumOfBuckets + ", SecondNumOfBuckets = " + secondNumOfBuckets);
		
		DataSourceComponent relation1 = new DataSourceComponent(firstCompName, dataPath
				+ firstSrcFile + extension).addOperator(projectionLineitem1).setHashIndexes(hash);
		_queryBuilder.add(relation1);
		
		DataSourceComponent relation2 = new DataSourceComponent(secondCompName, dataPath
				+ secondSrcFile + extension).addOperator(projectionLineitem2).setHashIndexes(hash);
		_queryBuilder.add(relation2);
		
		// equi-weight histogram
		// send something to the extra partitioner node
		relation1.setPartitioner(true);
		relation2.setPartitioner(true);
		// add operators which samples for partitioner
		SampleAsideAndForwardOperator saf1 = new SampleAsideAndForwardOperator(firstRelSize, firstNumOfBuckets, SystemParameters.PARTITIONER, conf);
		SampleAsideAndForwardOperator saf2 = new SampleAsideAndForwardOperator(secondRelSize, secondNumOfBuckets, SystemParameters.PARTITIONER, conf);
		relation1.addOperator(saf1);
		relation2.addOperator(saf2);
		
		// do we build d2 out of the first relation (_firstParent)?
		boolean isFirstD2 = SystemParameters.getBoolean(conf, "IS_FIRST_D2");
		EWHSampleMatrixComponent ewhComp = new EWHSampleMatrixComponent(relation1, relation2, isFirstD2, keyType, comparison,  
				numLastJoiners, firstRelSize, secondRelSize,
				firstNumOfBuckets, secondNumOfBuckets);
		_queryBuilder.add(ewhComp);
	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}