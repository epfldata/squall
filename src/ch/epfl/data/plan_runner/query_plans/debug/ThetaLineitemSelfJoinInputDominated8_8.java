package ch.epfl.data.plan_runner.query_plans.debug;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.plan_runner.components.theta.ThetaJoinDynamicComponentAdvisedEpochs;
import ch.epfl.data.plan_runner.components.theta.ThetaJoinStaticComponent;
import ch.epfl.data.plan_runner.conversion.DateConversion;
import ch.epfl.data.plan_runner.conversion.DateIntegerConversion;
import ch.epfl.data.plan_runner.conversion.DoubleConversion;
import ch.epfl.data.plan_runner.conversion.IntegerConversion;
import ch.epfl.data.plan_runner.conversion.StringConversion;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.expressions.ValueSpecification;
import ch.epfl.data.plan_runner.operators.AggregateCountOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.operators.SelectOperator;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.predicates.OrPredicate;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import ch.epfl.data.plan_runner.query_plans.theta.ThetaQueryPlansParameters;

public class ThetaLineitemSelfJoinInputDominated8_8 {

	/* For 0.01G
	 * Input: 15010 + 15010
	 * Output = 56346

	 */

	private QueryBuilder _queryBuilder = new QueryBuilder();
	private static final String _date1Str = "1993-06-17";
	private static final TypeConversion<Date> _dateConv = new DateConversion();
	//	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();   
	private static final Date _date1 = _dateConv.fromString(_date1Str);
	private static final TypeConversion<String> _stringConv = new StringConversion();

	private static final TypeConversion<Integer> _dateIntConv = new DateIntegerConversion();
	private static final IntegerConversion _ic = new IntegerConversion();
	private static final DoubleConversion _dblConv = new DoubleConversion();

	public ThetaLineitemSelfJoinInputDominated8_8(String dataPath, String extension, Map conf) {

		int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);

		//Project on shipdate, receiptdate, commitdate, shipInstruct, quantity, orderkey
		ProjectOperator projectionLineitem = new ProjectOperator(new int[] { 10, 12, 11, 13, 4, 0 });
		final List<Integer> hashLineitem = Arrays.asList(5);
		
		/*
		ComparisonPredicate comp1= new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, new ColumnReference(_stringConv, 14), new ValueSpecification(_stringConv, "TRUCK"));
		ComparisonPredicate comp2 = new ComparisonPredicate(ComparisonPredicate.GREATER_OP,new ColumnReference(_ic, 4), new ValueSpecification(_ic,0));
		AndPredicate and = new AndPredicate(comp1, comp2);
		*/
		ComparisonPredicate comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
				new ColumnReference(_stringConv, 13), new ValueSpecification(_stringConv, "NONE"));

		SelectOperator selectionOrders1 = new SelectOperator(comp);

		DataSourceComponent relationLineitem1 = new DataSourceComponent("LINEITEM1", dataPath
				+ "lineitem" + extension).add(selectionOrders1).add(
				projectionLineitem).setOutputPartKey(hashLineitem);
		_queryBuilder.add(relationLineitem1);

		//SelectOperator selectionOrders2 = new SelectOperator(new ComparisonPredicate(ComparisonPredicate.NONEQUAL_OP, new ColumnReference(_stringConv, 14), new ValueSpecification(_stringConv, "TRUCK")));
		ComparisonPredicate cond1 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
				new ColumnReference(_stringConv, 13), new ValueSpecification(_stringConv, "NONE"));
		ComparisonPredicate cond2 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
				new ColumnReference(_stringConv, 13), new ValueSpecification(_stringConv,
						"COLLECT COD"));
		OrPredicate and2 = new OrPredicate(cond1, cond2);
		//SelectOperator selectionOrders2 = new SelectOperator(and2);
		SelectOperator selectionOrders2 = new SelectOperator(cond1);
		DataSourceComponent relationLineitem2 = new DataSourceComponent("LINEITEM2", dataPath
				+ "lineitem" + extension).add(selectionOrders2).add(
				projectionLineitem).setOutputPartKey(hashLineitem);
		_queryBuilder.add(relationLineitem2);

		AggregateCountOperator agg = new AggregateCountOperator(conf);

		//		ColumnReference colLine11 = new ColumnReference(_dateIntConv, 0); //shipdate
		//		ColumnReference colLine12 = new ColumnReference(_dateConv, 1); //receiptdate
		ColumnReference colLine11 = new ColumnReference(_ic, 5); //orderkey

		//		ColumnReference colLine21 = new ColumnReference(_dateIntConv, 0);
		//		ColumnReference colLine22 = new ColumnReference(_dateConv, 1);
		ColumnReference colLine21 = new ColumnReference(_ic, 5); //orderkey

		//INTERVAL		
		//		IntervalPredicate pred3 = new IntervalPredicate(colLine11, colLine12, colLine22, colLine22);
		//		DateSum add2= new DateSum(colLine22, Calendar.DAY_OF_MONTH, 2);
		//		IntervalPredicate pred4 = new IntervalPredicate(colLine12, colLine12, colLine22, add2);

		//B+ TREE or Binary Tree
		/*
		 * |col1-col2|<=5
		 */
		ComparisonPredicate pred5 = new ComparisonPredicate(ComparisonPredicate.NONGREATER_OP,
				colLine11, colLine21, 1, ComparisonPredicate.BPLUSTREE);
		//ComparisonPredicate pred5 = new ComparisonPredicate(ComparisonPredicate.LESS_OP,colLine11, colLine21, 1, ComparisonPredicate.BPLUSTREE);
		//ComparisonPredicate pred5 = new ComparisonPredicate(ComparisonPredicate.NONGREATER_OP,colLine11, colLine21, 1, ComparisonPredicate.BINARYTREE);

		Component LINEITEMS_LINEITEMSjoin = ThetaJoinComponentFactory.createThetaJoinOperator(
				Theta_JoinType, relationLineitem1, relationLineitem2, _queryBuilder).setJoinPredicate(
				pred5).setContentSensitiveThetaJoinWrapper(_ic);
		//     .addOperator(agg)
		;

		LINEITEMS_LINEITEMSjoin.setPrintOut(false);

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}