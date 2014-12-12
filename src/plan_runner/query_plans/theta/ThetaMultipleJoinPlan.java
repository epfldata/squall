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
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.AndPredicate;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.query_plans.QueryPlan;

public class ThetaMultipleJoinPlan {

	private static Logger LOG = Logger.getLogger(ThetaMultipleJoinPlan.class);

	private final QueryPlan _queryPlan = new QueryPlan();

	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final NumericConversion<Integer> _intConv = new IntegerConversion();

	/*
	 * SELECT SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) FROM LINEITEM,
	 * ORDERS, NATION, SUPPLIER, PARTSUPP WHERE LINEITEM.ORDERKEY =
	 * ORDERS.ORDERKEY AND ORDERS.TOTALPRICE > 10*LINEITEM.EXTENDEDPRICE AND
	 * SUPPLIER.SUPPKEY = PARTSUPP.SUPPKEY AND PARTSUPP.PARTKEY =
	 * LINEITEM.PARTKEY AND PARTSUPP.SUPPKEY = LINEITEM.SUPPKEY AND
	 * PARTSUPP.AVAILQTY > 9990
	 */
	public ThetaMultipleJoinPlan(String dataPath, String extension, Map conf) {
		final int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
		// -------------------------------------------------------------------------------------
		final List<Integer> hashLineitem = Arrays.asList(0);

		final ProjectOperator projectionLineitem = new ProjectOperator(new int[] { 0, 1, 2, 5, 6 });

		final DataSourceComponent relationLineitem = new DataSourceComponent("LINEITEM", dataPath
				+ "lineitem" + extension, _queryPlan).setHashIndexes(hashLineitem).addOperator(
				projectionLineitem);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashOrders = Arrays.asList(0);

		final ProjectOperator projectionOrders = new ProjectOperator(new int[] { 0, 3 });

		final DataSourceComponent relationOrders = new DataSourceComponent("ORDERS", dataPath
				+ "orders" + extension, _queryPlan).setHashIndexes(hashOrders).addOperator(
				projectionOrders);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashSupplier = Arrays.asList(0);

		final ProjectOperator projectionSupplier = new ProjectOperator(new int[] { 0 });

		final DataSourceComponent relationSupplier = new DataSourceComponent("SUPPLIER", dataPath
				+ "supplier" + extension, _queryPlan).setHashIndexes(hashSupplier).addOperator(
				projectionSupplier);

		// -------------------------------------------------------------------------------------

		final List<Integer> hashPartsSupp = Arrays.asList(0);

		final ProjectOperator projectionPartsSupp = new ProjectOperator(new int[] { 0, 1, 2 });

		final DataSourceComponent relationPartsupp = new DataSourceComponent("PARTSUPP", dataPath
				+ "partsupp" + extension, _queryPlan).setHashIndexes(hashPartsSupp).addOperator(
				projectionPartsSupp);

		// -------------------------------------------------------------------------------------

		final ColumnReference colRefLineItem = new ColumnReference(_intConv, 0);
		final ColumnReference colRefOrders = new ColumnReference(_intConv, 0);
		final ComparisonPredicate predL_O1 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
				colRefLineItem, colRefOrders);

		final ColumnReference colRefLineItemExtPrice = new ColumnReference(_doubleConv, 3);
		final ColumnReference colRefOrdersTotalPrice = new ColumnReference(_doubleConv, 1);
		final ValueSpecification val10 = new ValueSpecification(_doubleConv, 10.0);
		final Multiplication mult = new Multiplication(val10, colRefLineItemExtPrice);
		final ComparisonPredicate predL_O2 = new ComparisonPredicate(ComparisonPredicate.LESS_OP,
				mult, colRefOrdersTotalPrice);

		final AndPredicate predL_O = new AndPredicate(predL_O1, predL_O2);

		Component LINEITEMS_ORDERSjoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationLineitem, relationOrders,
						_queryPlan).setJoinPredicate(predL_O)
				.addOperator(new ProjectOperator(new int[] { 1, 2, 3, 4 }));

		// -------------------------------------------------------------------------------------

		final SelectOperator selectionPartSupp = new SelectOperator(new ComparisonPredicate(
				ComparisonPredicate.GREATER_OP, new ColumnReference(_intConv, 2),
				new ValueSpecification(_intConv, 9990)));

		final ColumnReference colRefSupplier = new ColumnReference(_intConv, 0);
		final ColumnReference colRefPartSupp = new ColumnReference(_intConv, 1);
		final ComparisonPredicate predS_P = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
				colRefSupplier, colRefPartSupp);

		Component SUPPLIER_PARTSUPPjoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationSupplier, relationPartsupp,
						_queryPlan).setJoinPredicate(predS_P)
				.addOperator(new ProjectOperator(new int[] { 0, 1, 3 }))
				.addOperator(selectionPartSupp);

		// -------------------------------------------------------------------------------------

		// set up aggregation function on the StormComponent(Bolt) where join is
		// performed

		// 1 - discount
		final ValueExpression<Double> substract = new Subtraction(new ValueSpecification(
				_doubleConv, 1.0), new ColumnReference(_doubleConv, 3));
		// extendedPrice*(1-discount)
		final ValueExpression<Double> product = new Multiplication(new ColumnReference(_doubleConv,
				2), substract);
		final AggregateOperator agg = new AggregateSumOperator(product, conf);

		final ColumnReference colRefL_OPartKey = new ColumnReference(_intConv, 0);
		final ColumnReference colRefS_PPartKey = new ColumnReference(_intConv, 1);
		final ColumnReference colRefL_OSupKey = new ColumnReference(_intConv, 1);
		final ColumnReference colRefS_PSupKey = new ColumnReference(_intConv, 0);
		final ComparisonPredicate predL_P1 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
				colRefL_OPartKey, colRefS_PPartKey);
		final ComparisonPredicate predL_P2 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
				colRefL_OSupKey, colRefS_PSupKey);
		final AndPredicate predL_P = new AndPredicate(predL_P1, predL_P2);

		Component lastJoiner = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, LINEITEMS_ORDERSjoin,
						SUPPLIER_PARTSUPPjoin, _queryPlan).setJoinPredicate(predL_P)
				.addOperator(new ProjectOperator(new int[] { 0, 1, 2, 3 })).addOperator(agg);
		//lastJoiner.setPrintOut(false);
		// -------------------------------------------------------------------------------------

	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}