package plan_runner.query_plans.theta;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.theta.ThetaJoinComponentFactory;
import plan_runner.components.theta.ThetaJoinDynamicComponentAdvisedEpochs;
import plan_runner.components.theta.ThetaJoinStaticComponent;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.Multiplication;
import plan_runner.expressions.Subtraction;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.predicates.AndPredicate;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.query_plans.QueryBuilder;

public class ThetaInputDominatedPlan {

	private static Logger LOG = Logger.getLogger(ThetaInputDominatedPlan.class);

	private final QueryBuilder _queryBuilder = new QueryBuilder();

	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final IntegerConversion _ic = new IntegerConversion();

	/*
	 * SELECT SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) FROM LINEITEM,
	 * ORDERS WHERE LINEITEM.ORDERKEY = ORDERS.ORDERKEY AND ORDERS.TOTALPRICE >
	 * 10*LINEITEM.EXTENDEDPRICE
	 */
	public ThetaInputDominatedPlan(String dataPath, String extension, Map conf) {
		final int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
		// -------------------------------------------------------------------------------------
		final List<Integer> hashLineitem = Arrays.asList(0);

		final ProjectOperator projectionLineitem = new ProjectOperator(new int[] { 0, 5, 6 });

		final DataSourceComponent relationLineitem = new DataSourceComponent("LINEITEM", dataPath
				+ "lineitem" + extension).setHashIndexes(hashLineitem).addOperator(
				projectionLineitem);
		_queryBuilder.add(relationLineitem);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashOrders = Arrays.asList(1);

		final ProjectOperator projectionOrders = new ProjectOperator(new int[] { 0, 3 });

		final DataSourceComponent relationOrders = new DataSourceComponent("ORDERS", dataPath
				+ "orders" + extension).setHashIndexes(hashOrders).addOperator(
				projectionOrders);
		_queryBuilder.add(relationOrders);

		// -------------------------------------------------------------------------------------

		// set up aggregation function on the StormComponent(Bolt) where join is
		// performed
		// 1 - discount
		final ValueExpression<Double> substract = new Subtraction(new ValueSpecification(
				_doubleConv, 1.0), new ColumnReference(_doubleConv, 1));
		// extendedPrice*(1-discount)
		final ValueExpression<Double> product = new Multiplication(new ColumnReference(_doubleConv,
				0), substract);
		final AggregateOperator agg = new AggregateSumOperator(product, conf);

		// /Join predicate
		final ColumnReference colLineItems = new ColumnReference(_ic, 0);
		final ColumnReference colOrders = new ColumnReference(_ic, 0);
		final ComparisonPredicate pred1 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
				colLineItems, colOrders);

		final ValueSpecification value10 = new ValueSpecification(_doubleConv, 10.0);
		final ColumnReference colLineItemsInequality = new ColumnReference(_doubleConv, 1);
		final Multiplication mult = new Multiplication(value10, colLineItemsInequality);
		final ColumnReference colOrdersInequality = new ColumnReference(_doubleConv, 1);
		final ComparisonPredicate pred2 = new ComparisonPredicate(ComparisonPredicate.LESS_OP,
				mult, colOrdersInequality);

		final AndPredicate overallPred = new AndPredicate(pred1, pred2);

		Component lastJoiner = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationLineitem, relationOrders,
						_queryBuilder).setJoinPredicate(overallPred)
				.addOperator(new ProjectOperator(new int[] { 1, 2, 4 })).addOperator(agg);
		;

		//lastJoiner.setPrintOut(false);
		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}