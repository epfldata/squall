package plan_runner.query_plans.ewh;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.theta.ThetaJoinComponentFactory;
import plan_runner.components.theta.ThetaJoinDynamicComponentAdvisedEpochs;
import plan_runner.components.theta.ThetaJoinStaticComponent;
import plan_runner.conversion.DateConversion;
import plan_runner.conversion.DateIntegerConversion;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.LongConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.ewh.components.DummyComponent;
import plan_runner.expressions.Addition;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.DoubleToInt;
import plan_runner.expressions.IntegerYearFromDate;
import plan_runner.expressions.LongPhone;
import plan_runner.expressions.Multiplication;
import plan_runner.expressions.Subtraction;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateCountOperator;
import plan_runner.operators.PrintOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.AndPredicate;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.predicates.OrPredicate;
import plan_runner.query_plans.QueryBuilder;
import plan_runner.query_plans.theta.ThetaQueryPlansParameters;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.SystemParameters;
import plan_runner.utilities.SystemParameters.HistogramType;

// a candidate for new Eocd for the new Linux cluster
public class ThetaEWHCustomerJoin {
	
	private QueryBuilder _queryBuilder = new QueryBuilder();
	private static final TypeConversion<String> _stringConv = new StringConversion();
	private static final LongConversion _lc = new LongConversion();

	// phone and acctbal
	public ThetaEWHCustomerJoin(String dataPath, String extension, Map conf) {
		// creates materialized relations
		boolean printSelected = MyUtilities.isPrintFilteredLast(conf);
		String matName1 = "cphone_1";
		String matName2 = "cphone_2";
		PrintOperator print1 = printSelected? new PrintOperator(matName1 + extension, conf) : null;
		PrintOperator print2 = printSelected? new PrintOperator(matName2 + extension, conf) : null;
		// read from materialized relations
		boolean isMaterialized = SystemParameters.isExisting(conf, "DIP_MATERIALIZED") && SystemParameters.getBoolean(conf, "DIP_MATERIALIZED");
        boolean isOkcanSampling = SystemParameters.isExisting(conf, "DIP_SAMPLING") && SystemParameters.getBoolean(conf, "DIP_SAMPLING");
        boolean isEWHSampling = SystemParameters.isExisting(conf, "DIP_EWH_SAMPLING") && SystemParameters.getBoolean(conf, "DIP_EWH_SAMPLING");
		boolean isEWHD2Histogram = SystemParameters.getBooleanIfExist(conf, HistogramType.D2_COMB_HIST.genConfEntryName());
		boolean isEWHS1Histogram = SystemParameters.getBooleanIfExist(conf, HistogramType.S1_RES_HIST.genConfEntryName());
		boolean isSrcHistogram = isEWHD2Histogram || isEWHS1Histogram;
		
		Component relationCustomer1, relationCustomer2;
		//Project on phone(key), custkey and name
		
		// all this was with z1
		//ValueExpression keyField = new LongPhone(4, 6); // MB works perfectly - not enough output skew
		//ValueExpression keyField = new LongPhone(4, 5); // too large output + MB works perfectly - not enough output skew
		//ValueExpression keyField = new DoubleToInt(5); // acctbal: too large output
		//ValueExpression keyField = new DoubleToInt(5); // acctbal with selectivity 1 MKSEGMENT: too large output
		//ComparisonPredicate comp1 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
		//		new ColumnReference(_stringConv, 6), new ValueSpecification(_stringConv, "BUILDING"));
		//SelectOperator selectionCustomer1 = new SelectOperator(comp1);
		//ComparisonPredicate comp2 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
		//		new ColumnReference(_stringConv, 6), new ValueSpecification(_stringConv, "MACHINERY"));
		//SelectOperator selectionCustomer2 = new SelectOperator(comp2);
		
		//all this is with z2
		//ValueExpression keyField = new LongPhone(4, 7); // MBucket on 10G faster than 1Bucket - no output skew
		//ValueExpression keyField = new LongPhone(4, 6); // MBucket on 80G slower only 50% than 1Bucket - too large output (7291M)
		ValueExpression keyField = new LongPhone(4); //output = input
		
		ValueExpression custKey = new ColumnReference(_stringConv, 0);
		ValueExpression name = new ColumnReference(_stringConv, 1);
		ProjectOperator projectionCustomer = new ProjectOperator(keyField, custKey, name);


		
		final List<Integer> hashCustomer = Arrays.asList(0);
		
		if(!isMaterialized){
			relationCustomer1 = new DataSourceComponent("CUSTOMER1", dataPath
					+ "customer" + extension).add(print1).add(projectionCustomer).setOutputPartKey(hashCustomer);
			_queryBuilder.add(relationCustomer1);
			
			relationCustomer2 = new DataSourceComponent("CUSTOMER2", dataPath
					+ "customer" + extension).add(print2).add(projectionCustomer).setOutputPartKey(hashCustomer);
			_queryBuilder.add(relationCustomer2);
		}else{
			relationCustomer1 = new DataSourceComponent("CUSTOMER1", dataPath
					+ matName1 + extension).add(projectionCustomer).setOutputPartKey(hashCustomer);
			_queryBuilder.add(relationCustomer1);

			relationCustomer2 = new DataSourceComponent("CUSTOMER2", dataPath
					+ matName2 + extension).add(projectionCustomer).setOutputPartKey(hashCustomer);
			_queryBuilder.add(relationCustomer2);
		}

		NumericConversion keyType = (NumericConversion) _lc;
		ComparisonPredicate comparison = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP);
		int firstKeyProject = 0;
		int secondKeyProject = 0;
		
		if(printSelected){
			relationCustomer1.setPrintOut(false);
			relationCustomer2.setPrintOut(false);
		}else if(isSrcHistogram){
			_queryBuilder = MyUtilities.addSrcHistogram(relationCustomer1, firstKeyProject, relationCustomer2, secondKeyProject, 
					keyType, comparison, isEWHD2Histogram, isEWHS1Histogram, conf);
		}else if(isOkcanSampling){
			_queryBuilder = MyUtilities.addOkcanSampler(relationCustomer1, relationCustomer2, firstKeyProject, secondKeyProject,
					_queryBuilder, keyType, comparison, conf);
		}else if(isEWHSampling){
			_queryBuilder = MyUtilities.addEWHSampler(relationCustomer1, relationCustomer2, firstKeyProject, secondKeyProject,
					_queryBuilder, keyType, comparison, conf); 
		}else{
			final int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
			final ColumnReference colC1 = new ColumnReference(keyType, firstKeyProject);
			final ColumnReference colC2 = new ColumnReference(keyType, secondKeyProject);
			//Addition expr2 = new Addition(colO2, new ValueSpecification(_ic, keyOffset));
			final ComparisonPredicate C1_C2_comp = new ComparisonPredicate(
					ComparisonPredicate.EQUAL_OP, colC1, colC2);

			//AggregateCountOperator agg = new AggregateCountOperator(conf);
			Component lastJoiner = ThetaJoinComponentFactory
					.createThetaJoinOperator(Theta_JoinType, relationCustomer1, relationCustomer2, _queryBuilder)
					.setJoinPredicate(C1_C2_comp).setContentSensitiveThetaJoinWrapper(keyType);
			//.addOperator(agg)
			// lastJoiner.setPrintOut(false);
			
			DummyComponent dummy = new DummyComponent(lastJoiner, "DUMMY");
			_queryBuilder.add(dummy);
		}

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}