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
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.EquiJoinComponent;
import ch.epfl.data.squall.components.OperatorComponent;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.AggregateCountOperator;
import ch.epfl.data.squall.operators.AggregateSumOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.storage.AggregationStorage;
import ch.epfl.data.squall.types.DoubleType;
import ch.epfl.data.squall.types.LongType;
import ch.epfl.data.squall.types.StringType;

public class HyracksPreAggPlan extends QueryPlan {
	private static Logger LOG = Logger.getLogger(HyracksPreAggPlan.class);

	private final QueryBuilder _queryBuilder = new QueryBuilder();

	private static final DoubleType _dc = new DoubleType();
	private static final StringType _sc = new StringType();
	private static final LongType _lc = new LongType();

	public HyracksPreAggPlan(String dataPath, String extension, Map conf) {
		// -------------------------------------------------------------------------------------
		// start of query plan filling
		final ProjectOperator projectionCustomer = new ProjectOperator(
				new int[] { 0, 6 });
		final List<Integer> hashCustomer = Arrays.asList(0);
		final DataSourceComponent relationCustomer = new DataSourceComponent(
				"CUSTOMER", dataPath + "customer" + extension).add(
				projectionCustomer).setOutputPartKey(hashCustomer);
		_queryBuilder.add(relationCustomer);

		// -------------------------------------------------------------------------------------
		final ProjectOperator projectionOrders = new ProjectOperator(
				new int[] { 1 });
		final List<Integer> hashOrders = Arrays.asList(0);
		final DataSourceComponent relationOrders = new DataSourceComponent(
				"ORDERS", dataPath + "orders" + extension)
				.add(projectionOrders).setOutputPartKey(hashOrders);
		_queryBuilder.add(relationOrders);

		// -------------------------------------------------------------------------------------
		final ProjectOperator projFirstOut = new ProjectOperator(
				new ColumnReference(_sc, 1), new ValueSpecification(_sc, "1"));
		final ProjectOperator projSecondOut = new ProjectOperator(new int[] {
				1, 2 });
		final AggregationStorage secondJoinStorage = new AggregationStorage(
				new AggregateCountOperator(conf).setGroupByColumns(Arrays
						.asList(0)), _lc, conf, false);

		final List<Integer> hashIndexes = Arrays.asList(0);
		final EquiJoinComponent CUSTOMER_ORDERSjoin = new EquiJoinComponent(
				relationCustomer, relationOrders)
				.setFirstPreAggProj(projFirstOut)
				.setSecondPreAggProj(projSecondOut)
				.setSecondPreAggStorage(secondJoinStorage)
				.setOutputPartKey(hashIndexes);
		_queryBuilder.add(CUSTOMER_ORDERSjoin);

		// -------------------------------------------------------------------------------------
		final AggregateSumOperator agg = new AggregateSumOperator(
				new ColumnReference(_dc, 1), conf).setGroupByColumns(Arrays
				.asList(0));

		OperatorComponent oc = new OperatorComponent(CUSTOMER_ORDERSjoin,
				"COUNTAGG").add(agg);
		_queryBuilder.add(oc);

		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}

}
