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

public class ThetaEWHBandLineitemSelfOrderkeyJoin {
	private QueryBuilder _queryBuilder = new QueryBuilder();
	private static final TypeConversion<String> _stringConv = new StringConversion();
	private static final IntegerConversion _ic = new IntegerConversion();

	public ThetaEWHBandLineitemSelfOrderkeyJoin(String dataPath, String extension, Map conf) {
        // 0.25 LINEITEM * 0.25 LINEITEM on orderkey band
		// I = 2 * 15M = 30M; O = 3 * 15M * 1/4 * 4 = 45M
		// Variability is [0, 3] * skew, practically is too small
		// Okcan achieves perfect load-balancing
		
		// creates materialized relations
		boolean printSelected = MyUtilities.isPrintFilteredLast(conf);
		PrintOperator print1 = printSelected? new PrintOperator("bci_ewh_1.tbl", conf) : null;
		PrintOperator print2 = printSelected? new PrintOperator("bci_ewh_2.tbl", conf) : null;
		// read from materialized relations
		boolean isMaterialized = SystemParameters.isExisting(conf, "DIP_MATERIALIZED") && SystemParameters.getBoolean(conf, "DIP_MATERIALIZED");
        boolean isOkcanSampling = SystemParameters.isExisting(conf, "DIP_SAMPLING") && SystemParameters.getBoolean(conf, "DIP_SAMPLING");
        boolean isEWHSampling = SystemParameters.isExisting(conf, "DIP_EWH_SAMPLING") && SystemParameters.getBoolean(conf, "DIP_EWH_SAMPLING");
		
		Component relationLineitem1, relationLineitem2;
		//Project on shipdate , receiptdate, commitdate, shipInstruct, quantity
		ProjectOperator projectionLineitem = new ProjectOperator(new int[] {10, 12, 11, 13, 4, 0});
		final List<Integer> hashLineitem = Arrays.asList(5);
		
		if(!isMaterialized){
			SelectOperator selectionLineitem1 = new SelectOperator(new ComparisonPredicate(
					ComparisonPredicate.LESS_OP, new ColumnReference(_ic, 3),
					new ValueSpecification(_ic, 2)));
			relationLineitem1 = new DataSourceComponent("LINEITEM1", dataPath
					+ "lineitem" + extension).addOperator(selectionLineitem1).addOperator(print1).addOperator(
					projectionLineitem).setHashIndexes(hashLineitem);
			_queryBuilder.add(relationLineitem1);

			// 15 - 15 cond_same
			/*
					SelectOperator selectionLineitem2 = new SelectOperator(new ComparisonPredicate(
					ComparisonPredicate.LESS_OP, new ColumnReference(_ic, 3),
					new ValueSpecification(_ic, 2)));
			 */
			// 15 - 15 cond_diff
			
			ComparisonPredicate comp21 = new ComparisonPredicate(
					ComparisonPredicate.EQUAL_OP, new ColumnReference(_stringConv, 14),
					new ValueSpecification(_stringConv, "TRUCK"));
			ComparisonPredicate comp22 = new ComparisonPredicate(
					ComparisonPredicate.EQUAL_OP, new ColumnReference(_stringConv, 14),
					new ValueSpecification(_stringConv, "SHIP"));

			OrPredicate or2 = new OrPredicate(comp21, comp22);
			SelectOperator selectionLineitem2 = new SelectOperator(or2);

			relationLineitem2 = new DataSourceComponent("LINEITEM2", dataPath
					+ "lineitem" + extension).addOperator(selectionLineitem2).addOperator(print2).addOperator(
					projectionLineitem).setHashIndexes(hashLineitem);
			_queryBuilder.add(relationLineitem2);
		}else{
			relationLineitem1 = new DataSourceComponent("LINEITEM1", dataPath
					+ "bci_ewh_1" + extension).addOperator(projectionLineitem).setHashIndexes(hashLineitem);
			_queryBuilder.add(relationLineitem1);
			
			relationLineitem2 = new DataSourceComponent("LINEITEM2", dataPath
					+ "bci_ewh_2" + extension).addOperator(projectionLineitem).setHashIndexes(hashLineitem);
			_queryBuilder.add(relationLineitem2);
		}

		NumericConversion keyType = (NumericConversion) _ic;
		int comparisonValue = 1;
		ComparisonPredicate comparison = new ComparisonPredicate(ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP, comparisonValue, keyType);
		int firstKeyProject = 5;
		int secondKeyProject = 5;
		
		if(printSelected){
			relationLineitem1.setPrintOut(false);
			relationLineitem2.setPrintOut(false);
		}else if(isOkcanSampling){
			_queryBuilder = MyUtilities.addOkcanSampler(relationLineitem1, relationLineitem2, firstKeyProject, secondKeyProject,
					_queryBuilder, keyType, comparison, conf);
		}else if(isEWHSampling){
			_queryBuilder = MyUtilities.addEWHSampler(relationLineitem1, relationLineitem2, firstKeyProject, secondKeyProject,
					_queryBuilder, keyType, comparison, conf); 
		}else{
			int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
			boolean isBDB = MyUtilities.isBDB(conf);
			
			ColumnReference colLine11 = new ColumnReference(keyType, firstKeyProject); //shipdate
			//		ColumnReference colLine12 = new ColumnReference(_dateConv, 1); //receiptdate

			ColumnReference colLine21 = new ColumnReference(keyType, secondKeyProject);
			//		ColumnReference colLine22 = new ColumnReference(_dateConv, 1);

			//INTERVAL		
			//		IntervalPredicate pred3 = new IntervalPredicate(colLine11, colLine12, colLine22, colLine22);
			//		DateSum add2= new DateSum(colLine22, Calendar.DAY_OF_MONTH, 2);
			//		IntervalPredicate pred4 = new IntervalPredicate(colLine12, colLine12, colLine22, add2);

			//B+ TREE or Binary Tree
			// |col1-col2|<=5

			ComparisonPredicate pred5 = null;
			if (!isBDB) {
				pred5 = new ComparisonPredicate(ComparisonPredicate.NONGREATER_OP, colLine11,
						colLine21, comparisonValue, ComparisonPredicate.BPLUSTREE);
				//ComparisonPredicate pred5 = new ComparisonPredicate(ComparisonPredicate.LESS_OP,colLine11, colLine21, 1, ComparisonPredicate.BPLUSTREE);
				//ComparisonPredicate pred5 = new ComparisonPredicate(ComparisonPredicate.NONGREATER_OP,colLine11, colLine21, 1, ComparisonPredicate.BINARYTREE);
			} else {
				pred5 = new ComparisonPredicate(ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP, colLine11,
						colLine21, comparisonValue);
			}

			//AggregateCountOperator agg = new AggregateCountOperator(conf);		
			Component LINEITEMS_LINEITEMSjoin = ThetaJoinComponentFactory.createThetaJoinOperator(
					Theta_JoinType, relationLineitem1, relationLineitem2, _queryBuilder).setJoinPredicate(
							pred5).setContentSensitiveThetaJoinWrapper(keyType)
							;
			// .addOperator(agg)
			// LINEITEMS_LINEITEMSjoin.setPrintOut(false);
			
			DummyComponent dummy = new DummyComponent(LINEITEMS_LINEITEMSjoin, "DUMMY");
			_queryBuilder.add(dummy);
		}

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}