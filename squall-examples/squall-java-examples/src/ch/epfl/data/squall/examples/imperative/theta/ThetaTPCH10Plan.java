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
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.squall.conversion.DateConversion;
import ch.epfl.data.squall.conversion.DoubleConversion;
import ch.epfl.data.squall.conversion.IntegerConversion;
import ch.epfl.data.squall.conversion.NumericConversion;
import ch.epfl.data.squall.conversion.StringConversion;
import ch.epfl.data.squall.conversion.TypeConversion;
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

public class ThetaTPCH10Plan extends QueryPlan {
	private static void computeDates() {
		// date2= date1 + 3 months
		String date1Str = "1993-10-01";
		int interval = 3;
		int unit = Calendar.MONTH;

		// setting _date1
		_date1 = _dc.fromString(date1Str);

		// setting _date2
		ValueExpression<Date> date1Ve, date2Ve;
		date1Ve = new ValueSpecification<Date>(_dc, _date1);
		date2Ve = new DateSum(date1Ve, unit, interval);
		_date2 = date2Ve.eval(null);
		// tuple is set to null since we are computing based on constants
	}

	private static Logger LOG = Logger.getLogger(ThetaTPCH10Plan.class);
	private static final TypeConversion<Date> _dc = new DateConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final StringConversion _sc = new StringConversion();

	private QueryBuilder _queryBuilder = new QueryBuilder();

	private static final IntegerConversion _ic = new IntegerConversion();
	// query variables
	private static Date _date1, _date2;

	private static String MARK = "R";

	public ThetaTPCH10Plan(String dataPath, String extension, Map conf) {
		computeDates();

		int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);

		// -------------------------------------------------------------------------------------
		List<Integer> hashCustomer = Arrays.asList(0);

		ProjectOperator projectionCustomer = new ProjectOperator(new int[] { 0,
				1, 2, 3, 4, 5, 7 });

		DataSourceComponent relationCustomer = new DataSourceComponent(
				"CUSTOMER", dataPath + "customer" + extension)
				.setOutputPartKey(hashCustomer).add(projectionCustomer);
		_queryBuilder.add(relationCustomer);

		// -------------------------------------------------------------------------------------
		List<Integer> hashOrders = Arrays.asList(1);

		SelectOperator selectionOrders = new SelectOperator(
				new BetweenPredicate(new ColumnReference(_dc, 4), true,
						new ValueSpecification(_dc, _date1), false,
						new ValueSpecification(_dc, _date2)));

		ProjectOperator projectionOrders = new ProjectOperator(
				new int[] { 0, 1 });

		DataSourceComponent relationOrders = new DataSourceComponent("ORDERS",
				dataPath + "orders" + extension).setOutputPartKey(hashOrders)
				.add(selectionOrders).add(projectionOrders);
		_queryBuilder.add(relationOrders);

		// -------------------------------------------------------------------------------------
		ColumnReference colC = new ColumnReference(_ic, 0);
		ColumnReference colO = new ColumnReference(_ic, 1);
		ComparisonPredicate C_O_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colC, colO);

		Component C_Ojoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationCustomer,
						relationOrders, _queryBuilder)
				.setOutputPartKey(Arrays.asList(3))
				.add(new ProjectOperator(new int[] { 0, 1, 2, 3, 4, 5, 6, 7 }))
				.setJoinPredicate(C_O_comp);

		// -------------------------------------------------------------------------------------
		List<Integer> hashNation = Arrays.asList(0);

		ProjectOperator projectionNation = new ProjectOperator(
				new int[] { 0, 1 });

		DataSourceComponent relationNation = new DataSourceComponent("NATION",
				dataPath + "nation" + extension).setOutputPartKey(hashNation)
				.add(projectionNation);
		_queryBuilder.add(relationNation);
		// -------------------------------------------------------------------------------------

		ColumnReference colC_O = new ColumnReference(_ic, 3);
		ColumnReference colN = new ColumnReference(_ic, 0);
		ComparisonPredicate C_O_N_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colC_O, colN);

		Component C_O_Njoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, C_Ojoin,
						relationNation, _queryBuilder)
				.add(new ProjectOperator(new int[] { 0, 1, 2, 4, 5, 6, 7, 9 }))
				.setOutputPartKey(Arrays.asList(6))
				.setJoinPredicate(C_O_N_comp);

		// -------------------------------------------------------------------------------------

		List<Integer> hashLineitem = Arrays.asList(0);

		SelectOperator selectionLineitem = new SelectOperator(
				new ComparisonPredicate(new ColumnReference(_sc, 8),
						new ValueSpecification(_sc, MARK)));

		ProjectOperator projectionLineitem = new ProjectOperator(new int[] { 0,
				5, 6 });

		DataSourceComponent relationLineitem = new DataSourceComponent(
				"LINEITEM", dataPath + "lineitem" + extension)
				.setOutputPartKey(hashLineitem).add(selectionLineitem)
				.add(projectionLineitem);
		_queryBuilder.add(relationLineitem);

		// -------------------------------------------------------------------------------------
		// set up aggregation function on the StormComponent(Bolt) where join is
		// performed

		// 1 - discount
		ValueExpression<Double> substract = new Subtraction(
				new ValueSpecification(_doubleConv, 1.0), new ColumnReference(
						_doubleConv, 8));
		// extendedPrice*(1-discount)
		ValueExpression<Double> product = new Multiplication(
				new ColumnReference(_doubleConv, 7), substract);
		AggregateOperator agg = new AggregateSumOperator(product, conf)
		// .setGroupByColumns(Arrays.asList(0, 1, 4, 6, 2, 3, 5));
				.setGroupByColumns(Arrays.asList(0, 1, 4, 6, 2, 3, 5));

		ColumnReference colC_O_N = new ColumnReference(_ic, 6);
		ColumnReference colL = new ColumnReference(_ic, 0);
		ComparisonPredicate C_O_N_L_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colC_O_N, colL);

		Component C_O_N_Ljoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, C_O_Njoin,
						relationLineitem, _queryBuilder)
				.add(new ProjectOperator(
						new int[] { 0, 1, 2, 3, 4, 5, 7, 9, 10 })).add(agg)
				.setJoinPredicate(C_O_N_L_comp);

		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
