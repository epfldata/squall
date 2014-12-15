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
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.ewh.components.DummyComponent;
import plan_runner.expressions.Addition;
import plan_runner.expressions.ColumnReference;
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

//Eocd
public class ThetaEWHEquiOrdersCustkeyCustkeyJoin {
	private QueryBuilder _queryBuilder = new QueryBuilder();
	private static final TypeConversion<String> _stringConv = new StringConversion();
	private static final IntegerConversion _ic = new IntegerConversion();
	private DateIntegerConversion _dic = new DateIntegerConversion();

	public ThetaEWHEquiOrdersCustkeyCustkeyJoin(String dataPath, String extension, Map conf) {
        // ORDERS * ORDERS on orderkey equi
		// I = 2 * 15M = 30M; O = 15M * 10 = 150M
		// Variability is [0, 10] * skew  and is too small
		// baseline (z4 offset =0) takes forever to execute because of humongous output; there is output skew
		// baseline + offset=500 has 115M output, but Okcan does not have output skew
		// baseline + offset=1 has 115M output, but Okcan does not have output skew
		// baseline with z1 takes forever to execute due to humongous output
		// baseline + offset=1  (?)takes forever to execute due to humongous output
		// baseline + offset=1 + select disjoint has no output skew (30M output tuples in total)
		// baseline + offset=1 + selectKey too small output + no output skew
		// baseline + offset=1 + selectFirstOnly 45M output with no output skew
		// baseline + offset=1 + selectFirstOnly) urgent 60M with no output skew
		// baseline + z1 + select disjoint humongous output, a little bit of skew
		// baseline + z1 + select disjoin (6m + 3m): humongous output, there is skew
		// baseline + z1 + select date (4 and > 19960103, 1): (1.2m, 3m, 2m): small output
		// baseline + z1 + select date (< 19960101, 1-2): (8m, 6m, 5m): no output skew
// BEST baseline + z1 + select disjoin (3m + 3m (4, 1)): 240M output, there is output skew 
		       // (we are 5% better than 1Bucket and several time better than MBUcket!)
		       // orders_custkey_custkey_equi/orders_custkey_self_equi_16j_off0_z1_disjoint
		// GOOD baseline + z1 + select date (4 and < 19960103, 1): (1.9m, 3m, > 215m): there is good output skew
		// GOOD baseline + z1 + select date (4 and > 19960101, 1): (1.2m, 3m, seems similar to before
		// baseline + uniform + select disjoin (3m + 3m (4, 1)): 10M output, there is no (very little) output skew
		// baseline + z1 + select date (4 and < 19960101, 1): (1.7m, 3m, 5m): no output skew
     	       // (we are 2seconds better than MBucket and much better (several times) than 1Bucket!)
		
		// creates materialized relations
		boolean printSelected = MyUtilities.isPrintFilteredLast(conf);
		String matName1 = "bosc_1";
		String matName2 = "bosc_2";
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
		ProjectOperator projectionLineitem = new ProjectOperator(new int[] {0, 2, 3, 4, 5, 1});
		final List<Integer> hashLineitem = Arrays.asList(5);
		
		if(!isMaterialized){
			// ORDERDATE NO: startdate - enddate - 151, but for z4 mostly are 1996-01-02
			//STARTDATE = 1992-01-01 CURRENTDATE = 1995-06-17 ENDDATE = 1998-12-31
			/*
			Integer dateBoundary = 19960101;
			//ComparisonPredicate sel11 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
			//		new ColumnReference(_stringConv, 5), new ValueSpecification(_stringConv, "4-NOT SPECIFIED"));
			ComparisonPredicate sel12 = new ComparisonPredicate(ComparisonPredicate.LESS_OP,
					new ColumnReference(_dic, 4), new ValueSpecification(_dic, dateBoundary));
			//AndPredicate andOrders1 = new AndPredicate(sel11, sel12);
			SelectOperator selectionOrders1 = new SelectOperator(sel12);
			 */

			// 3m
			ComparisonPredicate sel11 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
					new ColumnReference(_stringConv, 5), new ValueSpecification(_stringConv, "4-NOT SPECIFIED"));
			SelectOperator selectionOrders1 = new SelectOperator(sel11);
			
			// 6M
			/*
			ComparisonPredicate sel11 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
					new ColumnReference(_stringConv, 5), new ValueSpecification(_stringConv, "4-NOT SPECIFIED"));
			ComparisonPredicate sel12 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
					new ColumnReference(_stringConv, 5), new ValueSpecification(_stringConv, "5-LOW"));
			OrPredicate orOrders1 = new OrPredicate(sel11, sel12);
			SelectOperator selectionOrders1 = new SelectOperator(sel11);
			*/
			
			//sel first urgent 9m
			/*
			ComparisonPredicate sel11 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
					new ColumnReference(_stringConv, 5), new ValueSpecification(_stringConv, "1-URGENT"));
			ComparisonPredicate sel12 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
					new ColumnReference(_stringConv, 5), new ValueSpecification(_stringConv, "2-HIGH"));
			ComparisonPredicate sel13 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
					new ColumnReference(_stringConv, 5), new ValueSpecification(_stringConv, "3-MEDIUM"));
			OrPredicate orOrders1 = new OrPredicate(sel11, sel12, sel13);
			SelectOperator selectionOrders1 = new SelectOperator(orOrders1);
			*/
			
			relationOrders1 = new DataSourceComponent("ORDERS1", dataPath
					+ "orders" + extension).addOperator(selectionOrders1).addOperator(print1).addOperator(
					projectionLineitem).setHashIndexes(hashLineitem);
			_queryBuilder.add(relationOrders1);

			//selectKey 8.5M
			/*
			SelectOperator selectionOrders2 = new SelectOperator(new ComparisonPredicate(
					ComparisonPredicate.LESS_OP, new ColumnReference(_ic, 0),
					new ValueSpecification(_ic, 8500000)));
			*/
			
			//disjoint 9M
			/*
			ComparisonPredicate sel21 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
					new ColumnReference(_stringConv, 5), new ValueSpecification(_stringConv, "1-URGENT"));
			ComparisonPredicate sel22 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
					new ColumnReference(_stringConv, 5), new ValueSpecification(_stringConv, "2-HIGH"));
			ComparisonPredicate sel23 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
					new ColumnReference(_stringConv, 5), new ValueSpecification(_stringConv, "3-MEDIUM"));
			OrPredicate orOrders2 = new OrPredicate(sel21, sel22, sel23);
			SelectOperator selectionOrders2 = new SelectOperator(sel21);
			*/
			
			// 3M
			ComparisonPredicate sel21 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
					new ColumnReference(_stringConv, 5), new ValueSpecification(_stringConv, "1-URGENT"));
	//		ComparisonPredicate sel22 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
//					new ColumnReference(_stringConv, 5), new ValueSpecification(_stringConv, "2-HIGH"));
			//OrPredicate orOrders2 = new OrPredicate(sel21, sel22);
			SelectOperator selectionOrders2 = new SelectOperator(sel21);
			
			
			relationOrders2 = new DataSourceComponent("ORDERS2", dataPath
					+ "orders" + extension).addOperator(selectionOrders2).addOperator(print2).addOperator(
					projectionLineitem).setHashIndexes(hashLineitem);
			_queryBuilder.add(relationOrders2);
		}else{
			relationOrders1 = new DataSourceComponent("ORDERS1", dataPath
					+ matName1 + extension).addOperator(projectionLineitem).setHashIndexes(hashLineitem);
			_queryBuilder.add(relationOrders1);

			relationOrders2 = new DataSourceComponent("LINEITEM2", dataPath
					+ matName2 + extension).addOperator(projectionLineitem).setHashIndexes(hashLineitem);
			_queryBuilder.add(relationOrders2);
		}

		//int keyOffset = 1;
		NumericConversion keyType = (NumericConversion) _ic;
		ComparisonPredicate comparison = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP);
		int firstKeyProject = 5;
		int secondKeyProject = 5;
		
		if(printSelected){
			relationOrders1.setPrintOut(false);
			relationOrders2.setPrintOut(false);
		}else if(isSrcHistogram){
			_queryBuilder = MyUtilities.addSrcHistogram(relationOrders1, firstKeyProject, relationOrders2, secondKeyProject, 
					keyType, comparison, isEWHD2Histogram, isEWHS1Histogram, conf);
		}else if(isOkcanSampling){
			_queryBuilder = MyUtilities.addOkcanSampler(relationOrders1, relationOrders2, firstKeyProject, secondKeyProject,
					_queryBuilder, keyType, comparison, conf);
		}else if(isEWHSampling){
			_queryBuilder = MyUtilities.addEWHSampler(relationOrders1, relationOrders2, firstKeyProject, secondKeyProject,
					_queryBuilder, keyType, comparison, conf); 
		}else{
			final int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
			final ColumnReference colO1 = new ColumnReference(keyType, firstKeyProject);
			final ColumnReference colO2 = new ColumnReference(keyType, secondKeyProject);
			//Addition expr2 = new Addition(colO2, new ValueSpecification(_ic, keyOffset));
			final ComparisonPredicate O1_O2_comp = new ComparisonPredicate(
					ComparisonPredicate.EQUAL_OP, colO1, colO2);

			//AggregateCountOperator agg = new AggregateCountOperator(conf);
			Component lastJoiner = ThetaJoinComponentFactory
					.createThetaJoinOperator(Theta_JoinType, relationOrders1, relationOrders2, _queryBuilder)
					.setJoinPredicate(O1_O2_comp).setContentSensitiveThetaJoinWrapper(keyType);
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