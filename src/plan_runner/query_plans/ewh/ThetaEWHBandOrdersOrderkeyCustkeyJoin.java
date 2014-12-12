package plan_runner.query_plans.ewh;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.theta.ThetaJoinComponentFactory;
import plan_runner.components.theta.ThetaJoinDynamicComponentAdvisedEpochs;
import plan_runner.components.theta.ThetaJoinStaticComponent;
import plan_runner.conversion.DateConversion;
import plan_runner.conversion.DateIntegerConversion;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.ewh.components.DummyComponent;
import plan_runner.expressions.Addition;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.Division;
import plan_runner.expressions.Multiplication;
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

public class ThetaEWHBandOrdersOrderkeyCustkeyJoin {
	private static Logger LOG = Logger.getLogger(ThetaEWHBandOrdersOrderkeyCustkeyJoin.class);	
	private QueryPlan _queryPlan = new QueryPlan();
	private static final TypeConversion<String> _stringConv = new StringConversion();
	private static final IntegerConversion _ic = new IntegerConversion();
	private DateIntegerConversion _dic = new DateIntegerConversion();

	//Bicb
	public ThetaEWHBandOrdersOrderkeyCustkeyJoin(String dataPath, String extension, Map conf) {
        // ORDERS * ORDERS on orderkey equi
		// I = 2 * 15M = 30M; O = 
		// Variability is [0, 10] * skew
		// baseline z1: MBucket 102s with 8 joiners doing nothing, EWH uses only 3-4 joiners due to large candidate no-output-sample rounded matrix cells
		// B: z1 + firstProject / 10: Output is around 120M tuples. From 283s to 235s (bsp-i). Should compare with 1B as this is output-dominated
		// C(based on A): z2 + secondProject * 10: Output is around 11M tuples. From 140s to 128s (bsp-i).
		// D(based on A): z3 + secondProject * 10: Output is around 11M tuples. From 153s to 147s (bsp-i).
  // BEST: E(based on A): z1 + secondProject * 10, comparisonValue = 2: Output is around 18M tuples. From 152s to 131s (bsp-i), great result!!!.
		        // in orders_orderkey_custkey_band/orderkey_custkey_band_16j_z1_abs2_project10
		// A: z1 + secondProject * 10: Output is around 11M tuples. From 120s to 105s (bsp-i), great result!
		// F(based on A): z0 + secondProject * 10, comparisonValue = 2: Output is around 18M tuples. From 134s to 122s (bsp-i).

		// creates materialized relations
		boolean printSelected = MyUtilities.isPrintFilteredLast(conf);
		String matName1 = "n_bbosc_1";
		String matName2 = "n_bbosc_2";
		PrintOperator print1 = printSelected? new PrintOperator(matName1 + extension, conf) : null;
		PrintOperator print2 = printSelected? new PrintOperator(matName2 + extension, conf) : null;
		// read from materialized relations
		boolean isMaterialized = SystemParameters.isExisting(conf, "DIP_MATERIALIZED") && SystemParameters.getBoolean(conf, "DIP_MATERIALIZED");
        boolean isOkcanSampling = SystemParameters.isExisting(conf, "DIP_SAMPLING") && SystemParameters.getBoolean(conf, "DIP_SAMPLING");
        boolean isEWHSampling = SystemParameters.isExisting(conf, "DIP_EWH_SAMPLING") && SystemParameters.getBoolean(conf, "DIP_EWH_SAMPLING");
		boolean isEWHD2Histogram = SystemParameters.getBooleanIfExist(conf, HistogramType.D2_COMB_HIST.genConfEntryName());
		boolean isEWHS1Histogram = SystemParameters.getBooleanIfExist(conf, HistogramType.S1_RES_HIST.genConfEntryName());
		boolean isSrcHistogram = isEWHD2Histogram || isEWHS1Histogram;
		
		Component relationOrders1, relationOrders2;
		//Project on shipdate , receiptdate, commitdate, shipInstruct, quantity
		ColumnReference col1 = new ColumnReference(_ic, 0);
		ColumnReference col2 = new ColumnReference(_ic, 2);
		ColumnReference col3 = new ColumnReference(_ic, 3);
		ColumnReference col4 = new ColumnReference(_ic, 4);
		ColumnReference col5 = new ColumnReference(_ic, 5);

		//A
		ColumnReference j1 = new ColumnReference(_ic, 0);
		ValueExpression j2 = new Multiplication(
				new ValueSpecification(_ic, 10),
				new ColumnReference(_ic, 1)
				);
		
		// B
		/*
		ValueExpression j1 = new Division(
				new ColumnReference(_ic, 0),
				new ValueSpecification(_ic, 10)
				);
		ColumnReference j2 = new ColumnReference(_ic, 1);
		*/
		
		ProjectOperator projectionLineitem1 = new ProjectOperator(col1, col2, col3, col4, col5, j1);
		ProjectOperator projectionLineitem2 = new ProjectOperator(col1, col2, col3, col4, col5, j2);
		//ProjectOperator projectionLineitem1 = new ProjectOperator(new int[] {0, 2, 3, 4, 5, 0});
		//ProjectOperator projectionLineitem2 = new ProjectOperator(new int[] {0, 2, 3, 4, 5, 1});
		final List<Integer> hashLineitem = Arrays.asList(5);
		
		if(!isMaterialized){
			relationOrders1 = new DataSourceComponent("ORDERS1", dataPath
					+ "orders" + extension, _queryPlan).addOperator(print1).addOperator(
					projectionLineitem1).setHashIndexes(hashLineitem);
			
			relationOrders2 = new DataSourceComponent("ORDERS2", dataPath
					+ "orders" + extension, _queryPlan).addOperator(print2).addOperator(
					projectionLineitem2).setHashIndexes(hashLineitem);
		}else{
			// WATCH OUT ON PROJECTIONS AFTER MATERIALIZATIONS
			relationOrders1 = new DataSourceComponent("ORDERS1", dataPath
					+ matName1 + extension, _queryPlan).addOperator(projectionLineitem1).setHashIndexes(hashLineitem);

			relationOrders2 = new DataSourceComponent("LINEITEM2", dataPath
					+ matName2 + extension, _queryPlan).addOperator(projectionLineitem2).setHashIndexes(hashLineitem);
		}
	
		
		NumericConversion keyType = (NumericConversion) _ic;
		int comparisonValue = 2;
		if (SystemParameters.isExisting(conf, "COMPARISON_VALUE")){
			comparisonValue = SystemParameters.getInt(conf, "COMPARISON_VALUE");
			LOG.info("ComparisonValue read from the config file: " + comparisonValue);
		}
		ComparisonPredicate comparison = new ComparisonPredicate(ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP, comparisonValue, keyType);
		int firstKeyProject = 5;
		int secondKeyProject = 5;
		
		/*
		ValueExpression ve1 = new ColumnReference(keyType, firstKeyProject);
		ValueExpression ve2 = new Multiplication(
				new ValueSpecification(keyType, secondMult),
				new ColumnReference(keyType, secondKeyProject));
		ProjectOperator project1 = new ProjectOperator(ve1);
		ProjectOperator project2 = new ProjectOperator(ve2);
		*/
		
		if(printSelected){
			relationOrders1.setPrintOut(false);
			relationOrders2.setPrintOut(false);
		}else if(isSrcHistogram){
			_queryPlan = MyUtilities.addSrcHistogram(relationOrders1, firstKeyProject, relationOrders2, secondKeyProject, 
					keyType, comparison, isEWHD2Histogram, isEWHS1Histogram, conf);
		}else if(isOkcanSampling){
			_queryPlan = MyUtilities.addOkcanSampler(relationOrders1, relationOrders2, firstKeyProject, secondKeyProject,
					_queryPlan, keyType, comparison, conf);
		}else if(isEWHSampling){
			_queryPlan = MyUtilities.addEWHSampler(relationOrders1, relationOrders2, firstKeyProject, secondKeyProject,
					_queryPlan, keyType, comparison, conf); 
		}else{
			final int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
			final ColumnReference colO1 = new ColumnReference(keyType, firstKeyProject);
			final ColumnReference colO2 = new ColumnReference(keyType, secondKeyProject);
			
			ComparisonPredicate pred5 = new ComparisonPredicate(ComparisonPredicate.NONGREATER_OP, colO1,
					colO2, comparisonValue, ComparisonPredicate.BPLUSTREE);

			//AggregateCountOperator agg = new AggregateCountOperator(conf);		
			Component lastJoiner = ThetaJoinComponentFactory.createThetaJoinOperator(
					Theta_JoinType, relationOrders1, relationOrders2, _queryPlan).setJoinPredicate(
							pred5).setContentSensitiveThetaJoinWrapper(keyType)
							;
			// .addOperator(agg)
			// lastJoiner.setPrintOut(false);
			
			DummyComponent dummy = new DummyComponent(lastJoiner, "DUMMY", _queryPlan);
		}

	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}