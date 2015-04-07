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
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.Multiplication;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.AggregateSumOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.types.DoubleType;
import ch.epfl.data.squall.types.IntegerType;
import ch.epfl.data.squall.types.NumericType;

public class RSTPlan extends QueryPlan {
	private static Logger LOG = Logger.getLogger(RSTPlan.class);

	private static final NumericType<Double> _dc = new DoubleType();
	private static final NumericType<Integer> _ic = new IntegerType();

	private final QueryBuilder _queryBuilder = new QueryBuilder();

	public RSTPlan(String dataPath, String extension, Map conf) {
		// -------------------------------------------------------------------------------------
		// start of query plan filling
		final List<Integer> hashR = Arrays.asList(1);

		final DataSourceComponent relationR = new DataSourceComponent("R",
				dataPath + "r" + extension).setOutputPartKey(hashR);
		_queryBuilder.add(relationR);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashS = Arrays.asList(0);

		final DataSourceComponent relationS = new DataSourceComponent("S",
				dataPath + "s" + extension).setOutputPartKey(hashS);
		_queryBuilder.add(relationS);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashIndexes = Arrays.asList(2);

		final EquiJoinComponent R_Sjoin = new EquiJoinComponent(relationR,
				relationS).setOutputPartKey(hashIndexes);
		_queryBuilder.add(R_Sjoin);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashT = Arrays.asList(0);

		final DataSourceComponent relationT = new DataSourceComponent("T",
				dataPath + "t" + extension).setOutputPartKey(hashT);
		_queryBuilder.add(relationT);

		// -------------------------------------------------------------------------------------
		final ValueExpression<Double> aggVe = new Multiplication(
				new ColumnReference(_dc, 0), new ColumnReference(_dc, 3));

		final AggregateSumOperator sp = new AggregateSumOperator(aggVe, conf);

		EquiJoinComponent rstJoin = new EquiJoinComponent(R_Sjoin, relationT)
				.add(new SelectOperator(new ComparisonPredicate(
						new ColumnReference(_ic, 1), new ValueSpecification(
								_ic, 10)))).add(sp);
		_queryBuilder.add(rstJoin);
	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
