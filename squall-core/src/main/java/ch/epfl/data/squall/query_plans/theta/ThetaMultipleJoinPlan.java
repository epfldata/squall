/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package ch.epfl.data.squall.query_plans.theta;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.squall.conversion.DoubleConversion;
import ch.epfl.data.squall.conversion.IntegerConversion;
import ch.epfl.data.squall.conversion.NumericConversion;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.Multiplication;
import ch.epfl.data.squall.expressions.Subtraction;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.AggregateSumOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.AndPredicate;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;

public class ThetaMultipleJoinPlan extends QueryPlan {

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
		final int Theta_JoinType = ThetaQueryPlansParameters
				.getThetaJoinType(conf);
		// -------------------------------------------------------------------------------------
		final List<Integer> hashLineitem = Arrays.asList(0);

		final ProjectOperator projectionLineitem = new ProjectOperator(
				new int[] { 0, 1, 2, 5, 6 });

		final DataSourceComponent relationLineitem = new DataSourceComponent(
				"LINEITEM", dataPath + "lineitem" + extension)
				.setOutputPartKey(hashLineitem).add(projectionLineitem);
		_queryBuilder.add(relationLineitem);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashOrders = Arrays.asList(0);

		final ProjectOperator projectionOrders = new ProjectOperator(new int[] {
				0, 3 });

		final DataSourceComponent relationOrders = new DataSourceComponent(
				"ORDERS", dataPath + "orders" + extension).setOutputPartKey(
				hashOrders).add(projectionOrders);
		_queryBuilder.add(relationOrders);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashSupplier = Arrays.asList(0);

		final ProjectOperator projectionSupplier = new ProjectOperator(
				new int[] { 0 });

		final DataSourceComponent relationSupplier = new DataSourceComponent(
				"SUPPLIER", dataPath + "supplier" + extension)
				.setOutputPartKey(hashSupplier).add(projectionSupplier);
		_queryBuilder.add(relationSupplier);

		// -------------------------------------------------------------------------------------

		final List<Integer> hashPartsSupp = Arrays.asList(0);

		final ProjectOperator projectionPartsSupp = new ProjectOperator(
				new int[] { 0, 1, 2 });

		final DataSourceComponent relationPartsupp = new DataSourceComponent(
				"PARTSUPP", dataPath + "partsupp" + extension)
				.setOutputPartKey(hashPartsSupp).add(projectionPartsSupp);
		_queryBuilder.add(relationPartsupp);

		// -------------------------------------------------------------------------------------

		final ColumnReference colRefLineItem = new ColumnReference(_intConv, 0);
		final ColumnReference colRefOrders = new ColumnReference(_intConv, 0);
		final ComparisonPredicate predL_O1 = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colRefLineItem, colRefOrders);

		final ColumnReference colRefLineItemExtPrice = new ColumnReference(
				_doubleConv, 3);
		final ColumnReference colRefOrdersTotalPrice = new ColumnReference(
				_doubleConv, 1);
		final ValueSpecification val10 = new ValueSpecification(_doubleConv,
				10.0);
		final Multiplication mult = new Multiplication(val10,
				colRefLineItemExtPrice);
		final ComparisonPredicate predL_O2 = new ComparisonPredicate(
				ComparisonPredicate.LESS_OP, mult, colRefOrdersTotalPrice);

		final AndPredicate predL_O = new AndPredicate(predL_O1, predL_O2);

		Component LINEITEMS_ORDERSjoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationLineitem,
						relationOrders, _queryBuilder)
				.setJoinPredicate(predL_O)
				.add(new ProjectOperator(new int[] { 1, 2, 3, 4 }));

		// -------------------------------------------------------------------------------------

		final SelectOperator selectionPartSupp = new SelectOperator(
				new ComparisonPredicate(ComparisonPredicate.GREATER_OP,
						new ColumnReference(_intConv, 2),
						new ValueSpecification(_intConv, 9990)));

		final ColumnReference colRefSupplier = new ColumnReference(_intConv, 0);
		final ColumnReference colRefPartSupp = new ColumnReference(_intConv, 1);
		final ComparisonPredicate predS_P = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colRefSupplier, colRefPartSupp);

		Component SUPPLIER_PARTSUPPjoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationSupplier,
						relationPartsupp, _queryBuilder)
				.setJoinPredicate(predS_P)
				.add(new ProjectOperator(new int[] { 0, 1, 3 }))
				.add(selectionPartSupp);

		// -------------------------------------------------------------------------------------

		// set up aggregation function on the StormComponent(Bolt) where join is
		// performed

		// 1 - discount
		final ValueExpression<Double> substract = new Subtraction(
				new ValueSpecification(_doubleConv, 1.0), new ColumnReference(
						_doubleConv, 3));
		// extendedPrice*(1-discount)
		final ValueExpression<Double> product = new Multiplication(
				new ColumnReference(_doubleConv, 2), substract);
		final AggregateOperator agg = new AggregateSumOperator(product, conf);

		final ColumnReference colRefL_OPartKey = new ColumnReference(_intConv,
				0);
		final ColumnReference colRefS_PPartKey = new ColumnReference(_intConv,
				1);
		final ColumnReference colRefL_OSupKey = new ColumnReference(_intConv, 1);
		final ColumnReference colRefS_PSupKey = new ColumnReference(_intConv, 0);
		final ComparisonPredicate predL_P1 = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colRefL_OPartKey,
				colRefS_PPartKey);
		final ComparisonPredicate predL_P2 = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colRefL_OSupKey, colRefS_PSupKey);
		final AndPredicate predL_P = new AndPredicate(predL_P1, predL_P2);

		Component lastJoiner = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, LINEITEMS_ORDERSjoin,
						SUPPLIER_PARTSUPPjoin, _queryBuilder)
				.setJoinPredicate(predL_P)
				.add(new ProjectOperator(new int[] { 0, 1, 2, 3 })).add(agg);
		// lastJoiner.setPrintOut(false);
		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
