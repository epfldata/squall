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
import java.util.Date;
import java.util.List;
import java.util.Map;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.squall.conversion.DateConversion;
import ch.epfl.data.squall.conversion.DoubleConversion;
import ch.epfl.data.squall.conversion.NumericConversion;
import ch.epfl.data.squall.conversion.StringConversion;
import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.AggregateCountOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.ThetaQueryPlansParameters;

public class ThetaLineitemPricesSelfJoin {

	private QueryBuilder _queryBuilder = new QueryBuilder();
	private static final String _date1Str = "1993-06-17";
	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final Date _date1 = _dateConv.fromString(_date1Str);
	private static final TypeConversion<String> _stringConv = new StringConversion();

	public ThetaLineitemPricesSelfJoin(String dataPath, String extension,
			Map conf) {
		int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);

		ProjectOperator projectionLineitem = new ProjectOperator(new int[] { 4,
				5 });
		final List<Integer> hashLineitem1 = Arrays.asList(1);

		SelectOperator selectionLineitem1 = new SelectOperator(
				new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
						new ColumnReference(_stringConv, 14),
						new ValueSpecification(_stringConv, "TRUCK")));
		DataSourceComponent relationLineitem1 = new DataSourceComponent(
				"LINEITEM1", dataPath + "lineitem" + extension)
				.add(selectionLineitem1).add(projectionLineitem)
				.setOutputPartKey(hashLineitem1);
		_queryBuilder.add(relationLineitem1);

		SelectOperator selectionLinitem2 = new SelectOperator(
				new ComparisonPredicate(ComparisonPredicate.NONEQUAL_OP,
						new ColumnReference(_stringConv, 14),
						new ValueSpecification(_stringConv, "TRUCK")));
		final List<Integer> hashLineitem2 = Arrays.asList(0);
		DataSourceComponent relationLineitem2 = new DataSourceComponent(
				"LINEITEM2", dataPath + "lineitem" + extension)
				.add(selectionLinitem2).add(projectionLineitem)
				.setOutputPartKey(hashLineitem2);
		_queryBuilder.add(relationLineitem2);

		AggregateCountOperator agg = new AggregateCountOperator(conf);

		ColumnReference colLine1 = new ColumnReference(_doubleConv, 1); // prices
		ColumnReference colLine12 = new ColumnReference(_doubleConv, 0); // quantity

		/*
		 * Division div1 = new Division(colLine1, colLine12); ColumnReference
		 * colLine2 = new ColumnReference(_doubleConv, 1); ColumnReference
		 * colLine22 = new ColumnReference(_doubleConv, 0); Division div2 = new
		 * Division(colLine2, colLine22);
		 */

		// INTERVAL
		// Addition add = new Addition(colLine2, new
		// ValueSpecification(_doubleConv,2.0));
		// Subtraction sub = new Subtraction(colLine2, new
		// ValueSpecification(_doubleConv,2.0));

		// IntervalPredicate pred3 = new IntervalPredicate(colLine1, colLine1,
		// sub, add);

		// Addition add = new Addition(colLine2, new
		// ValueSpecification(_doubleConv,10.0));
		ComparisonPredicate pred3 = new ComparisonPredicate(
				ComparisonPredicate.LESS_OP, colLine1, colLine12, 10,
				ComparisonPredicate.BPLUSTREE);

		// Addition add = new Addition(div2, new
		// ValueSpecification(_doubleConv,5.0));
		// ComparisonPredicate pred3 = new
		// ComparisonPredicate(ComparisonPredicate.LESS_OP,div1, add);

		Component LINEITEMS_LINEITEMSjoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationLineitem1,
						relationLineitem2, _queryBuilder)
				.setJoinPredicate(pred3).add(agg)
				.setContentSensitiveThetaJoinWrapper(_doubleConv);

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
