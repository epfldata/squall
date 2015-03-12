package ch.epfl.data.plan_runner.query_plans.theta;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.plan_runner.components.theta.ThetaJoinStaticComponent;
import ch.epfl.data.plan_runner.conversion.DateConversion;
import ch.epfl.data.plan_runner.conversion.DoubleConversion;
import ch.epfl.data.plan_runner.conversion.NumericConversion;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.expressions.Addition;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.expressions.Multiplication;
import ch.epfl.data.plan_runner.expressions.Subtraction;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.expressions.ValueSpecification;
import ch.epfl.data.plan_runner.operators.AggregateAvgOperator;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.operators.SelectOperator;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;

public class ThetaOrdersSelfJoin {

	private QueryBuilder _queryBuilder = new QueryBuilder();
	private static final String _date1Str = "1993-06-17";
	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final Date _date1 = _dateConv.fromString(_date1Str);

	public ThetaOrdersSelfJoin(String dataPath, String extension, Map conf) {
		int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);

		double value = 7000.0;

		ComparisonPredicate comp1 = new ComparisonPredicate(ComparisonPredicate.LESS_OP,
				new ColumnReference(_dateConv, 4), new ValueSpecification(_dateConv, _date1));

		ComparisonPredicate comp2 = new ComparisonPredicate(ComparisonPredicate.GREATER_OP,
				new ColumnReference(_doubleConv, 1), new ValueSpecification(_doubleConv, value));

		SelectOperator selectionOrders1 = new SelectOperator(comp1);
		final List<Integer> hashLineitem = Arrays.asList(1);

		//		SelectOperator selectionOrders1 = new SelectOperator(new AndPredicate(comp1, comp2));

		DataSourceComponent relationOrders1 = new DataSourceComponent("ORDERS1", dataPath
				+ "orders" + extension).add(selectionOrders1).add(
				new ProjectOperator(new int[] { 0, 3 })).setOutputPartKey(hashLineitem);
		_queryBuilder.add(relationOrders1);

		SelectOperator selectionOrders2 = new SelectOperator(new ComparisonPredicate(
				ComparisonPredicate.NONLESS_OP, new ColumnReference(_dateConv, 4),
				new ValueSpecification(_dateConv, _date1)));

		DataSourceComponent relationOrders2 = new DataSourceComponent("ORDERS2", dataPath
				+ "orders" + extension).add(selectionOrders2).add(
				new ProjectOperator(new int[] { 0, 3 })).setOutputPartKey(hashLineitem);
		_queryBuilder.add(relationOrders2);

		//Aggregate
		ValueExpression<Double> substract = new Subtraction(new ColumnReference(_doubleConv, 1),
				new ColumnReference(_doubleConv, 3));
		AggregateOperator agg = new AggregateAvgOperator(substract, conf);

		//Join Predicate
		ColumnReference colOrders1 = new ColumnReference(_doubleConv, 1);
		ColumnReference colOrders2 = new ColumnReference(_doubleConv, 1);

		Addition add = new Addition(colOrders1, new ValueSpecification(_doubleConv, value));
		Subtraction sub = new Subtraction(colOrders1, new ValueSpecification(_doubleConv, value));
		//		ComparisonPredicate pred1 = new ComparisonPredicate(ComparisonPredicate.GREATER_OP, add, colOrders2);
		ComparisonPredicate pred2 = new ComparisonPredicate(ComparisonPredicate.LESS_OP, sub,
				colOrders2);
		//		ComparisonPredicate pred2 = new ComparisonPredicate(ComparisonPredicate.GREATER_OP, colOrders1, colOrders2);

		Multiplication mult = new Multiplication(colOrders2, new ValueSpecification(_doubleConv,
				10.0));

		ComparisonPredicate pred3 = new ComparisonPredicate(ComparisonPredicate.GREATER_OP,
				colOrders1, mult);

		Component ORDERS_ORDERSjoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationOrders1, relationOrders2,
						_queryBuilder).setJoinPredicate(pred3).add(agg).setContentSensitiveThetaJoinWrapper(_doubleConv);
	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}

}