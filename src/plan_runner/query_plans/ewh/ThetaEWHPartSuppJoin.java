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
import plan_runner.query_plans.QueryPlan;
import plan_runner.query_plans.theta.ThetaQueryPlansParameters;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.SystemParameters;
import plan_runner.utilities.SystemParameters.HistogramType;

// a candidate for new Eocd for the new Linux cluster
public class ThetaEWHPartSuppJoin {
	
	private QueryPlan _queryPlan = new QueryPlan();
	private static final TypeConversion<String> _stringConv = new StringConversion();
	private static final IntegerConversion _ic = new IntegerConversion();

	// availqty
	public ThetaEWHPartSuppJoin(String dataPath, String extension, Map conf) {
		// creates materialized relations
		boolean printSelected = MyUtilities.isPrintFilteredLast(conf);
		String matName1 = "partsupp_qty_1";
		String matName2 = "partsupp_qty_2";
		PrintOperator print1 = printSelected? new PrintOperator(matName1 + extension, conf) : null;
		PrintOperator print2 = printSelected? new PrintOperator(matName2 + extension, conf) : null;
		// read from materialized relations
		boolean isMaterialized = SystemParameters.isExisting(conf, "DIP_MATERIALIZED") && SystemParameters.getBoolean(conf, "DIP_MATERIALIZED");
        boolean isOkcanSampling = SystemParameters.isExisting(conf, "DIP_SAMPLING") && SystemParameters.getBoolean(conf, "DIP_SAMPLING");
        boolean isEWHSampling = SystemParameters.isExisting(conf, "DIP_EWH_SAMPLING") && SystemParameters.getBoolean(conf, "DIP_EWH_SAMPLING");
		boolean isEWHD2Histogram = SystemParameters.getBooleanIfExist(conf, HistogramType.D2_COMB_HIST.genConfEntryName());
		boolean isEWHS1Histogram = SystemParameters.getBooleanIfExist(conf, HistogramType.S1_RES_HIST.genConfEntryName());
		boolean isSrcHistogram = isEWHD2Histogram || isEWHS1Histogram;
		
		Component relationPartSupp1, relationPartSupp2;
		//Project on availqty(key), partkey and suppkey
		ProjectOperator projectionCustomer = new ProjectOperator(new int[] {2, 0, 1});

		//with z2 too large output
		
		final List<Integer> hashCustomer = Arrays.asList(0);
		
		if(!isMaterialized){
			relationPartSupp1 = new DataSourceComponent("PARTSUPP1", dataPath
					+ "partsupp" + extension, _queryPlan).addOperator(print1).addOperator(projectionCustomer).setHashIndexes(hashCustomer);
			
			relationPartSupp2 = new DataSourceComponent("PARTSUPP2", dataPath
					+ "partsupp" + extension, _queryPlan).addOperator(print2).addOperator(projectionCustomer).setHashIndexes(hashCustomer);
		}else{
			relationPartSupp1 = new DataSourceComponent("PARTSUPP1", dataPath
					+ matName1 + extension, _queryPlan).addOperator(projectionCustomer).setHashIndexes(hashCustomer);

			relationPartSupp2 = new DataSourceComponent("PARTSUPP2", dataPath
					+ matName2 + extension, _queryPlan).addOperator(projectionCustomer).setHashIndexes(hashCustomer);
		}

		NumericConversion keyType = (NumericConversion) _ic;
		ComparisonPredicate comparison = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP);
		int firstKeyProject = 0;
		int secondKeyProject = 0;
		
		if(printSelected){
			relationPartSupp1.setPrintOut(false);
			relationPartSupp2.setPrintOut(false);
		}else if(isSrcHistogram){
			_queryPlan = MyUtilities.addSrcHistogram(relationPartSupp1, firstKeyProject, relationPartSupp2, secondKeyProject, 
					keyType, comparison, isEWHD2Histogram, isEWHS1Histogram, conf);
		}else if(isOkcanSampling){
			_queryPlan = MyUtilities.addOkcanSampler(relationPartSupp1, relationPartSupp2, firstKeyProject, secondKeyProject,
					_queryPlan, keyType, comparison, conf);
		}else if(isEWHSampling){
			_queryPlan = MyUtilities.addEWHSampler(relationPartSupp1, relationPartSupp2, firstKeyProject, secondKeyProject,
					_queryPlan, keyType, comparison, conf); 
		}else{
			final int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
			final ColumnReference colPS1 = new ColumnReference(keyType, firstKeyProject);
			final ColumnReference colPS2 = new ColumnReference(keyType, secondKeyProject);
			//Addition expr2 = new Addition(colO2, new ValueSpecification(_ic, keyOffset));
			final ComparisonPredicate PS1_PS2_comp = new ComparisonPredicate(
					ComparisonPredicate.EQUAL_OP, colPS1, colPS2);

			//AggregateCountOperator agg = new AggregateCountOperator(conf);
			Component lastJoiner = ThetaJoinComponentFactory
					.createThetaJoinOperator(Theta_JoinType, relationPartSupp1, relationPartSupp2, _queryPlan)
					.setJoinPredicate(PS1_PS2_comp).setContentSensitiveThetaJoinWrapper(keyType);
			//.addOperator(agg)
			// lastJoiner.setPrintOut(false);
			
			DummyComponent dummy = new DummyComponent(lastJoiner, "DUMMY", _queryPlan);
		}

	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}