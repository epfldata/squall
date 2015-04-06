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


package ch.epfl.data.squall.examples.imperative;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.EquiJoinComponent;
import ch.epfl.data.squall.conversion.DateConversion;
import ch.epfl.data.squall.conversion.DoubleConversion;
import ch.epfl.data.squall.conversion.NumericConversion;
import ch.epfl.data.squall.conversion.StringConversion;
import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.IntegerYearFromDate;
import ch.epfl.data.squall.expressions.Multiplication;
import ch.epfl.data.squall.expressions.Subtraction;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.AggregateSumOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.LikePredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;

public class TPCH9Plan extends QueryPlan {
	private static Logger LOG = Logger.getLogger(TPCH9Plan.class);

	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final StringConversion _sc = new StringConversion();

	private static final String COLOR = "%green%";

	private final QueryBuilder _queryBuilder = new QueryBuilder();

	public TPCH9Plan(String dataPath, String extension, Map conf) {
		// -------------------------------------------------------------------------------------
		final List<Integer> hashPart = Arrays.asList(0);

		final SelectOperator selectionPart = new SelectOperator(
				new LikePredicate(new ColumnReference(_sc, 1),
						new ValueSpecification(_sc, COLOR)));

		final ProjectOperator projectionPart = new ProjectOperator(
				new int[] { 0 });

		final DataSourceComponent relationPart = new DataSourceComponent(
				"PART", dataPath + "part" + extension)
				.setOutputPartKey(hashPart).add(selectionPart)
				.add(projectionPart);
		_queryBuilder.add(relationPart);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashLineitem = Arrays.asList(1);

		final ProjectOperator projectionLineitem = new ProjectOperator(
				new int[] { 0, 1, 2, 4, 5, 6 });

		final DataSourceComponent relationLineitem = new DataSourceComponent(
				"LINEITEM", dataPath + "lineitem" + extension)
				.setOutputPartKey(hashLineitem).add(projectionLineitem);
		_queryBuilder.add(relationLineitem);

		// -------------------------------------------------------------------------------------
		final EquiJoinComponent P_Ljoin = new EquiJoinComponent(relationPart,
				relationLineitem).setOutputPartKey(Arrays.asList(0, 2));
		_queryBuilder.add(P_Ljoin);
		// -------------------------------------------------------------------------------------

		final List<Integer> hashPartsupp = Arrays.asList(0, 1);

		final ProjectOperator projectionPartsupp = new ProjectOperator(
				new int[] { 0, 1, 3 });

		final DataSourceComponent relationPartsupp = new DataSourceComponent(
				"PARTSUPP", dataPath + "partsupp" + extension)
				.setOutputPartKey(hashPartsupp).add(projectionPartsupp);
		_queryBuilder.add(relationPartsupp);

		// -------------------------------------------------------------------------------------
		final EquiJoinComponent P_L_PSjoin = new EquiJoinComponent(P_Ljoin,
				relationPartsupp).setOutputPartKey(Arrays.asList(0)).add(
				new ProjectOperator(new int[] { 1, 2, 3, 4, 5, 6 }));
		_queryBuilder.add(P_L_PSjoin);
		// -------------------------------------------------------------------------------------

		final List<Integer> hashOrders = Arrays.asList(0);

		final ProjectOperator projectionOrders = new ProjectOperator(
				new ColumnReference(_sc, 0), new IntegerYearFromDate(
						new ColumnReference(_dateConv, 4)));

		final DataSourceComponent relationOrders = new DataSourceComponent(
				"ORDERS", dataPath + "orders" + extension).setOutputPartKey(
				hashOrders).add(projectionOrders);
		_queryBuilder.add(relationOrders);

		// -------------------------------------------------------------------------------------

		final EquiJoinComponent P_L_PS_Ojoin = new EquiJoinComponent(
				P_L_PSjoin, relationOrders).setOutputPartKey(Arrays.asList(0))
				.add(new ProjectOperator(new int[] { 1, 2, 3, 4, 5, 6 }));
		_queryBuilder.add(P_L_PS_Ojoin);
		// -------------------------------------------------------------------------------------

		final List<Integer> hashSupplier = Arrays.asList(0);

		final ProjectOperator projectionSupplier = new ProjectOperator(
				new int[] { 0, 3 });

		final DataSourceComponent relationSupplier = new DataSourceComponent(
				"SUPPLIER", dataPath + "supplier" + extension)
				.setOutputPartKey(hashSupplier).add(projectionSupplier);
		_queryBuilder.add(relationSupplier);

		// -------------------------------------------------------------------------------------
		final EquiJoinComponent P_L_PS_O_Sjoin = new EquiJoinComponent(
				P_L_PS_Ojoin, relationSupplier).setOutputPartKey(
				Arrays.asList(5)).add(
				new ProjectOperator(new int[] { 1, 2, 3, 4, 5, 6 }));
		_queryBuilder.add(P_L_PS_O_Sjoin);
		// -------------------------------------------------------------------------------------
		final List<Integer> hashNation = Arrays.asList(0);

		final ProjectOperator projectionNation = new ProjectOperator(new int[] {
				0, 1 });

		final DataSourceComponent relationNation = new DataSourceComponent(
				"NATION", dataPath + "nation" + extension).setOutputPartKey(
				hashNation).add(projectionNation);
		_queryBuilder.add(relationNation);

		// -------------------------------------------------------------------------------------
		// set up aggregation function on the StormComponent(Bolt) where join is
		// performed

		// 1 - discount
		final ValueExpression<Double> substract1 = new Subtraction(
				new ValueSpecification(_doubleConv, 1.0), new ColumnReference(
						_doubleConv, 2));
		// extendedPrice*(1-discount)
		final ValueExpression<Double> product1 = new Multiplication(
				new ColumnReference(_doubleConv, 1), substract1);

		// ps_supplycost * l_quantity
		final ValueExpression<Double> product2 = new Multiplication(
				new ColumnReference(_doubleConv, 3), new ColumnReference(
						_doubleConv, 0));

		// all together
		final ValueExpression<Double> substract2 = new Subtraction(product1,
				product2);

		final AggregateOperator agg = new AggregateSumOperator(substract2, conf)
				.setGroupByColumns(Arrays.asList(5, 4));

		EquiJoinComponent finalComp = new EquiJoinComponent(P_L_PS_O_Sjoin,
				relationNation).add(
				new ProjectOperator(new int[] { 0, 1, 2, 3, 4, 6 })).add(agg);
		_queryBuilder.add(finalComp);

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
