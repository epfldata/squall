package plan_runner.query_plans.theta;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.theta.ThetaJoinComponentFactory;
import plan_runner.components.theta.ThetaJoinStaticComponent;
import plan_runner.conversion.DateConversion;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.Division;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateCountOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.query_plans.QueryBuilder;

public class ThetaLineitemPricesSelfJoin {

	private QueryBuilder _queryPlan = new QueryBuilder();
	private static final String _date1Str = "1993-06-17";
	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final Date _date1 = _dateConv.fromString(_date1Str);
	private static final TypeConversion<String> _stringConv = new StringConversion();

	public ThetaLineitemPricesSelfJoin(String dataPath, String extension, Map conf) {
		int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);

		ProjectOperator projectionLineitem = new ProjectOperator(new int[] { 4, 5 });
		final List<Integer> hashLineitem1 = Arrays.asList(1);
		
		SelectOperator selectionLineitem1 = new SelectOperator(new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, new ColumnReference(_stringConv, 14),
				new ValueSpecification(_stringConv, "TRUCK")));
		DataSourceComponent relationLineitem1 = new DataSourceComponent("LINEITEM1", dataPath
				+ "lineitem" + extension, _queryPlan).addOperator(selectionLineitem1).addOperator(
				projectionLineitem).setHashIndexes(hashLineitem1);

		SelectOperator selectionLinitem2 = new SelectOperator(new ComparisonPredicate(
				ComparisonPredicate.NONEQUAL_OP, new ColumnReference(_stringConv, 14),
				new ValueSpecification(_stringConv, "TRUCK")));
		final List<Integer> hashLineitem2 = Arrays.asList(0);
		DataSourceComponent relationLineitem2 = new DataSourceComponent("LINEITEM2", dataPath
				+ "lineitem" + extension, _queryPlan).addOperator(selectionLinitem2).addOperator(
				projectionLineitem).setHashIndexes(hashLineitem2);

		AggregateCountOperator agg = new AggregateCountOperator(conf);

		ColumnReference colLine1 = new ColumnReference(_doubleConv, 1); //prices
		ColumnReference colLine12 = new ColumnReference(_doubleConv, 0); //quantity

		/*
		Division div1 = new Division(colLine1, colLine12);
		ColumnReference colLine2 = new ColumnReference(_doubleConv, 1);
		ColumnReference colLine22 = new ColumnReference(_doubleConv, 0);
		Division div2 = new Division(colLine2, colLine22);
		*/

		//INTERVAL
		//		Addition add = new Addition(colLine2, new ValueSpecification(_doubleConv,2.0));
		//		Subtraction sub = new Subtraction(colLine2, new ValueSpecification(_doubleConv,2.0));

		//IntervalPredicate pred3 = new IntervalPredicate(colLine1, colLine1, sub, add);

		//		Addition add = new Addition(colLine2, new ValueSpecification(_doubleConv,10.0));
		ComparisonPredicate pred3 = new ComparisonPredicate(ComparisonPredicate.LESS_OP, colLine1,
				colLine12, 10, ComparisonPredicate.BPLUSTREE);

		//		Addition add = new Addition(div2, new ValueSpecification(_doubleConv,5.0));
		//		ComparisonPredicate pred3 = new ComparisonPredicate(ComparisonPredicate.LESS_OP,div1, add);

		Component LINEITEMS_LINEITEMSjoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationLineitem1, relationLineitem2,
						_queryPlan).setJoinPredicate(pred3).addOperator(agg).setContentSensitiveThetaJoinWrapper(_doubleConv);

	}

	public QueryBuilder getQueryPlan() {
		return _queryPlan;
	}
}
