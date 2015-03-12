package ch.epfl.data.plan_runner.query_plans.theta;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.plan_runner.components.theta.ThetaJoinDynamicComponentAdvisedEpochs;
import ch.epfl.data.plan_runner.components.theta.ThetaJoinStaticComponent;
import ch.epfl.data.plan_runner.conversion.DoubleConversion;
import ch.epfl.data.plan_runner.conversion.IntegerConversion;
import ch.epfl.data.plan_runner.conversion.NumericConversion;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.expressions.Multiplication;
import ch.epfl.data.plan_runner.expressions.Subtraction;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.expressions.ValueSpecification;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.AggregateSumOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.operators.SelectOperator;
import ch.epfl.data.plan_runner.predicates.AndPredicate;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;

public class ThetaMultipleJoinPlan {

	private static Logger LOG = Logger.getLogger(ThetaMultipleJoinPlan.class);

	private final QueryBuilder _queryBuilder = new QueryBuilder();

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
				+ "lineitem" + extension).setOutputPartKey(hashLineitem).add(
				projectionLineitem);
		_queryBuilder.add(relationLineitem);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashOrders = Arrays.asList(0);

		final ProjectOperator projectionOrders = new ProjectOperator(new int[] { 0, 3 });

		final DataSourceComponent relationOrders = new DataSourceComponent("ORDERS", dataPath
				+ "orders" + extension).setOutputPartKey(hashOrders).add(
				projectionOrders);
		_queryBuilder.add(relationOrders);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashSupplier = Arrays.asList(0);

		final ProjectOperator projectionSupplier = new ProjectOperator(new int[] { 0 });

		final DataSourceComponent relationSupplier = new DataSourceComponent("SUPPLIER", dataPath
				+ "supplier" + extension).setOutputPartKey(hashSupplier).add(
				projectionSupplier);
		_queryBuilder.add(relationSupplier);

		// -------------------------------------------------------------------------------------

		final List<Integer> hashPartsSupp = Arrays.asList(0);

		final ProjectOperator projectionPartsSupp = new ProjectOperator(new int[] { 0, 1, 2 });

		final DataSourceComponent relationPartsupp = new DataSourceComponent("PARTSUPP", dataPath
				+ "partsupp" + extension).setOutputPartKey(hashPartsSupp).add(
				projectionPartsSupp);		
		_queryBuilder.add(relationPartsupp);

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
						_queryBuilder).setJoinPredicate(predL_O)
				.add(new ProjectOperator(new int[] { 1, 2, 3, 4 }));

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
						_queryBuilder).setJoinPredicate(predS_P)
				.add(new ProjectOperator(new int[] { 0, 1, 3 }))
				.add(selectionPartSupp);

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
						SUPPLIER_PARTSUPPjoin, _queryBuilder).setJoinPredicate(predL_P)
				.add(new ProjectOperator(new int[] { 0, 1, 2, 3 })).add(agg);
		//lastJoiner.setPrintOut(false);
		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}