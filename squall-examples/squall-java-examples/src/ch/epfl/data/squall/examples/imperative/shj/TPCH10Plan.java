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

package ch.epfl.data.squall.examples.imperative.shj;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.EquiJoinComponent;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.DateSum;
import ch.epfl.data.squall.expressions.Multiplication;
import ch.epfl.data.squall.expressions.Subtraction;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.AggregateSumOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.BetweenPredicate;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.types.DateType;
import ch.epfl.data.squall.types.DoubleType;
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.types.StringType;
import ch.epfl.data.squall.types.Type;

public class TPCH10Plan extends QueryPlan {
    private static void computeDates() {
	// date2= date1 + 3 months
	final String date1Str = "1993-10-01";
	final int interval = 3;
	final int unit = Calendar.MONTH;

	// setting _date1
	_date1 = _dc.fromString(date1Str);

	// setting _date2
	ValueExpression<Date> date1Ve, date2Ve;
	date1Ve = new ValueSpecification<Date>(_dc, _date1);
	date2Ve = new DateSum(date1Ve, unit, interval);
	_date2 = date2Ve.eval(null);
	// tuple is set to null since we are computing based on constants
    }

    private static Logger LOG = Logger.getLogger(TPCH10Plan.class);
    private static final Type<Date> _dc = new DateType();
    private static final NumericType<Double> _doubleConv = new DoubleType();
    private static final StringType _sc = new StringType();

    private final QueryBuilder _queryBuilder = new QueryBuilder();

    // query variables
    private static Date _date1, _date2;

    public TPCH10Plan(String dataPath, String extension, Map conf) {
	computeDates();

	// -------------------------------------------------------------------------------------
	final List<Integer> hashCustomer = Arrays.asList(0);

	final ProjectOperator projectionCustomer = new ProjectOperator(
		new int[] { 0, 1, 2, 3, 4, 5, 7 });

	final DataSourceComponent relationCustomer = new DataSourceComponent(
		"CUSTOMER", dataPath + "customer" + extension)
		.setOutputPartKey(hashCustomer).add(projectionCustomer);
	_queryBuilder.add(relationCustomer);

	// -------------------------------------------------------------------------------------
	final List<Integer> hashOrders = Arrays.asList(1);

	final SelectOperator selectionOrders = new SelectOperator(
		new BetweenPredicate(new ColumnReference(_dc, 4), true,
			new ValueSpecification(_dc, _date1), false,
			new ValueSpecification(_dc, _date2)));

	final ProjectOperator projectionOrders = new ProjectOperator(new int[] {
		0, 1 });

	final DataSourceComponent relationOrders = new DataSourceComponent(
		"ORDERS", dataPath + "orders" + extension)
		.setOutputPartKey(hashOrders).add(selectionOrders)
		.add(projectionOrders);
	_queryBuilder.add(relationOrders);

	// -------------------------------------------------------------------------------------
	final EquiJoinComponent C_Ojoin = new EquiJoinComponent(
		relationCustomer, relationOrders).setOutputPartKey(Arrays
		.asList(3));
	_queryBuilder.add(C_Ojoin);
	// -------------------------------------------------------------------------------------
	final List<Integer> hashNation = Arrays.asList(0);

	final ProjectOperator projectionNation = new ProjectOperator(new int[] {
		0, 1 });

	final DataSourceComponent relationNation = new DataSourceComponent(
		"NATION", dataPath + "nation" + extension).setOutputPartKey(
		hashNation).add(projectionNation);
	_queryBuilder.add(relationNation);
	// -------------------------------------------------------------------------------------

	final EquiJoinComponent C_O_Njoin = new EquiJoinComponent(C_Ojoin,
		relationNation).add(
		new ProjectOperator(new int[] { 0, 1, 2, 4, 5, 6, 7, 8 }))
		.setOutputPartKey(Arrays.asList(6));
	_queryBuilder.add(C_O_Njoin);
	// -------------------------------------------------------------------------------------

	final List<Integer> hashLineitem = Arrays.asList(0);

	final SelectOperator selectionLineitem = new SelectOperator(
		new ComparisonPredicate(new ColumnReference(_sc, 8),
			new ValueSpecification(_sc, "R")));

	final ProjectOperator projectionLineitem = new ProjectOperator(
		new int[] { 0, 5, 6 });

	final DataSourceComponent relationLineitem = new DataSourceComponent(
		"LINEITEM", dataPath + "lineitem" + extension)
		.setOutputPartKey(hashLineitem).add(selectionLineitem)
		.add(projectionLineitem);
	_queryBuilder.add(relationLineitem);

	// -------------------------------------------------------------------------------------
	// set up aggregation function on the StormComponent(Bolt) where join is
	// performed

	// 1 - discount
	final ValueExpression<Double> substract = new Subtraction(
		new ValueSpecification(_doubleConv, 1.0), new ColumnReference(
			_doubleConv, 8));
	// extendedPrice*(1-discount)
	final ValueExpression<Double> product = new Multiplication(
		new ColumnReference(_doubleConv, 7), substract);
	final AggregateOperator agg = new AggregateSumOperator(product, conf)
		.setGroupByColumns(Arrays.asList(0, 1, 4, 6, 2, 3, 5));

	EquiJoinComponent finalComp = new EquiJoinComponent(C_O_Njoin,
		relationLineitem).add(
		new ProjectOperator(new int[] { 0, 1, 2, 3, 4, 5, 7, 8, 9 }))
		.add(agg);
	_queryBuilder.add(finalComp);

    }

    @Override
    public QueryBuilder getQueryPlan() {
	return _queryBuilder;
    }
}
