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


package ch.epfl.data.squall.examples.imperative.debug;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.EquiJoinComponent;
import ch.epfl.data.squall.components.OperatorComponent;
import ch.epfl.data.squall.conversion.DateConversion;
import ch.epfl.data.squall.conversion.DoubleConversion;
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

public class TPCH5_CustomPlan extends QueryPlan {
	private static void computeDates() {
		// date2 = date1 + 1 year
		String date1Str = "1994-01-01";
		int interval = 1;
		int unit = Calendar.YEAR;

		// setting _date1
		_date1 = _dc.fromString(date1Str);

		// setting _date2
		ValueExpression<Date> date1Ve, date2Ve;
		date1Ve = new ValueSpecification<Date>(_dc, _date1);
		date2Ve = new DateSum(date1Ve, unit, interval);
		_date2 = date2Ve.eval(null);
		// tuple is set to null since we are computing based on constants
	}

	private static Logger LOG = Logger.getLogger(TPCH5_CustomPlan.class);
	private static final TypeConversion<Date> _dc = new DateConversion();
	private static final TypeConversion<String> _sc = new StringConversion();

	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();

	private QueryBuilder _queryBuilder = new QueryBuilder();
	// query variables
	private static Date _date1, _date2;

	// private static final String REGION_NAME = "ASIA";
	private static final String REGION_NAME = "AMERICA";

	public TPCH5_CustomPlan(String dataPath, String extension, Map conf) {
		computeDates();

		// -------------------------------------------------------------------------------------
		List<Integer> hashRegion = Arrays.asList(0);

		SelectOperator selectionRegion = new SelectOperator(
				new ComparisonPredicate(new ColumnReference(_sc, 1),
						new ValueSpecification(_sc, REGION_NAME)));

		ProjectOperator projectionRegion = new ProjectOperator(new int[] { 0 });

		DataSourceComponent relationRegion = new DataSourceComponent("REGION",
				dataPath + "region" + extension).setOutputPartKey(hashRegion)
				.add(selectionRegion).add(projectionRegion);
		_queryBuilder.add(relationRegion);

		// -------------------------------------------------------------------------------------
		List<Integer> hashNation = Arrays.asList(2);

		ProjectOperator projectionNation = new ProjectOperator(new int[] { 0,
				1, 2 });

		DataSourceComponent relationNation = new DataSourceComponent("NATION",
				dataPath + "nation" + extension).setOutputPartKey(hashNation)
				.add(projectionNation);
		_queryBuilder.add(relationNation);

		// -------------------------------------------------------------------------------------
		List<Integer> hashRN = Arrays.asList(0);

		ProjectOperator projectionRN = new ProjectOperator(new int[] { 1, 2 });

		EquiJoinComponent R_Njoin = new EquiJoinComponent(relationRegion,
				relationNation).setOutputPartKey(hashRN).add(projectionRN);
		_queryBuilder.add(R_Njoin);

		// -------------------------------------------------------------------------------------
		List<Integer> hashSupplier = Arrays.asList(1);

		ProjectOperator projectionSupplier = new ProjectOperator(new int[] { 0,
				3 });

		DataSourceComponent relationSupplier = new DataSourceComponent(
				"SUPPLIER", dataPath + "supplier" + extension)
				.setOutputPartKey(hashSupplier).add(projectionSupplier);
		_queryBuilder.add(relationSupplier);

		// -------------------------------------------------------------------------------------
		List<Integer> hashRNS = Arrays.asList(2);

		ProjectOperator projectionRNS = new ProjectOperator(
				new int[] { 0, 1, 2 });

		EquiJoinComponent R_N_Sjoin = new EquiJoinComponent(R_Njoin,
				relationSupplier).setOutputPartKey(hashRNS).add(projectionRNS);
		_queryBuilder.add(R_N_Sjoin);

		// -------------------------------------------------------------------------------------
		List<Integer> hashLineitem = Arrays.asList(1);

		ProjectOperator projectionLineitem = new ProjectOperator(new int[] { 0,
				2, 5, 6 });

		DataSourceComponent relationLineitem = new DataSourceComponent(
				"LINEITEM", dataPath + "lineitem" + extension)
				.setOutputPartKey(hashLineitem).add(projectionLineitem);
		_queryBuilder.add(relationLineitem);

		// -------------------------------------------------------------------------------------
		List<Integer> hashRNSL = Arrays.asList(0, 2);

		ProjectOperator projectionRNSL = new ProjectOperator(new int[] { 0, 1,
				3, 4, 5 });

		// AggregateCountOperator agg= new AggregateCountOperator(conf);

		EquiJoinComponent R_N_S_Ljoin = new EquiJoinComponent(R_N_Sjoin,
				relationLineitem).setOutputPartKey(hashRNSL)
				.add(projectionRNSL)
		// .addOperator(agg)
		;
		_queryBuilder.add(R_N_S_Ljoin);

		// -------------------------------------------------------------------------------------
		List<Integer> hashCustomer = Arrays.asList(0);

		ProjectOperator projectionCustomer = new ProjectOperator(new int[] { 0,
				3 });

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
		List<Integer> hashCO = Arrays.asList(0, 1);

		ProjectOperator projectionCO = new ProjectOperator(new int[] { 1, 2 });

		EquiJoinComponent C_Ojoin = new EquiJoinComponent(relationCustomer,
				relationOrders).setOutputPartKey(hashCO).add(projectionCO);
		_queryBuilder.add(C_Ojoin);

		// -------------------------------------------------------------------------------------
		List<Integer> hashRNSLCO = Arrays.asList(0);

		ProjectOperator projectionRNSLCO = new ProjectOperator(new int[] { 1,
				3, 4 });

		EquiJoinComponent R_N_S_L_C_Ojoin = new EquiJoinComponent(R_N_S_Ljoin,
				C_Ojoin).setOutputPartKey(hashRNSLCO).add(projectionRNSLCO);
		_queryBuilder.add(R_N_S_L_C_Ojoin);

		// -------------------------------------------------------------------------------------
		// set up aggregation function on a separate StormComponent(Bolt)

		ValueExpression<Double> substract = new Subtraction(
				new ValueSpecification(_doubleConv, 1.0), new ColumnReference(
						_doubleConv, 2));
		// extendedPrice*(1-discount)
		ValueExpression<Double> product = new Multiplication(
				new ColumnReference(_doubleConv, 1), substract);

		AggregateOperator aggOp = new AggregateSumOperator(product, conf)
				.setGroupByColumns(Arrays.asList(0));
		OperatorComponent finalComponent = new OperatorComponent(
				R_N_S_L_C_Ojoin, "FINAL_RESULT").add(aggOp);
		_queryBuilder.add(finalComponent);

		// -------------------------------------------------------------------------------------
	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
