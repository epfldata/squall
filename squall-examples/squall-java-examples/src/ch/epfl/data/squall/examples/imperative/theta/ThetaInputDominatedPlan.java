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


package ch.epfl.data.squall.examples.imperative.theta;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.Multiplication;
import ch.epfl.data.squall.expressions.Subtraction;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.AggregateSumOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.predicates.AndPredicate;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.query_plans.ThetaQueryPlansParameters;
import ch.epfl.data.squall.types.DoubleType;
import ch.epfl.data.squall.types.IntegerType;
import ch.epfl.data.squall.types.NumericType;

public class ThetaInputDominatedPlan extends QueryPlan {

	private static Logger LOG = Logger.getLogger(ThetaInputDominatedPlan.class);

	private final QueryBuilder _queryBuilder = new QueryBuilder();

	private static final NumericType<Double> _doubleConv = new DoubleType();
	private static final IntegerType _ic = new IntegerType();

	/*
	 * SELECT SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) FROM LINEITEM,
	 * ORDERS WHERE LINEITEM.ORDERKEY = ORDERS.ORDERKEY AND ORDERS.TOTALPRICE >
	 * 10*LINEITEM.EXTENDEDPRICE
	 */
	public ThetaInputDominatedPlan(String dataPath, String extension, Map conf) {
		final int Theta_JoinType = ThetaQueryPlansParameters
				.getThetaJoinType(conf);
		// -------------------------------------------------------------------------------------
		final List<Integer> hashLineitem = Arrays.asList(0);

		final ProjectOperator projectionLineitem = new ProjectOperator(
				new int[] { 0, 5, 6 });

		final DataSourceComponent relationLineitem = new DataSourceComponent(
				"LINEITEM", dataPath + "lineitem" + extension)
				.setOutputPartKey(hashLineitem).add(projectionLineitem);
		_queryBuilder.add(relationLineitem);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashOrders = Arrays.asList(1);

		final ProjectOperator projectionOrders = new ProjectOperator(new int[] {
				0, 3 });

		final DataSourceComponent relationOrders = new DataSourceComponent(
				"ORDERS", dataPath + "orders" + extension).setOutputPartKey(
				hashOrders).add(projectionOrders);
		_queryBuilder.add(relationOrders);

		// -------------------------------------------------------------------------------------

		// set up aggregation function on the StormComponent(Bolt) where join is
		// performed
		// 1 - discount
		final ValueExpression<Double> substract = new Subtraction(
				new ValueSpecification(_doubleConv, 1.0), new ColumnReference(
						_doubleConv, 1));
		// extendedPrice*(1-discount)
		final ValueExpression<Double> product = new Multiplication(
				new ColumnReference(_doubleConv, 0), substract);
		final AggregateOperator agg = new AggregateSumOperator(product, conf);

		// /Join predicate
		final ColumnReference colLineItems = new ColumnReference(_ic, 0);
		final ColumnReference colOrders = new ColumnReference(_ic, 0);
		final ComparisonPredicate pred1 = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colLineItems, colOrders);

		final ValueSpecification value10 = new ValueSpecification(_doubleConv,
				10.0);
		final ColumnReference colLineItemsInequality = new ColumnReference(
				_doubleConv, 1);
		final Multiplication mult = new Multiplication(value10,
				colLineItemsInequality);
		final ColumnReference colOrdersInequality = new ColumnReference(
				_doubleConv, 1);
		final ComparisonPredicate pred2 = new ComparisonPredicate(
				ComparisonPredicate.LESS_OP, mult, colOrdersInequality);

		final AndPredicate overallPred = new AndPredicate(pred1, pred2);

		Component lastJoiner = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationLineitem,
						relationOrders, _queryBuilder)
				.setJoinPredicate(overallPred)
				.add(new ProjectOperator(new int[] { 1, 2, 4 })).add(agg);
		;

		// lastJoiner.setPrintOut(false);
		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
