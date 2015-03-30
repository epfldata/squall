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
import java.util.Date;
import java.util.List;
import java.util.Map;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.squall.conversion.DateConversion;
import ch.epfl.data.squall.conversion.DoubleConversion;
import ch.epfl.data.squall.conversion.NumericConversion;
import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.expressions.Addition;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.Multiplication;
import ch.epfl.data.squall.expressions.Subtraction;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.AggregateAvgOperator;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;

public class ThetaOrdersSelfJoin {

	private QueryBuilder _queryBuilder = new QueryBuilder();
	private static final String _date1Str = "1993-06-17";
	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final Date _date1 = _dateConv.fromString(_date1Str);

	public ThetaOrdersSelfJoin(String dataPath, String extension, Map conf) {
		int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);

		double value = 7000.0;

		ComparisonPredicate comp1 = new ComparisonPredicate(
				ComparisonPredicate.LESS_OP, new ColumnReference(_dateConv, 4),
				new ValueSpecification(_dateConv, _date1));

		ComparisonPredicate comp2 = new ComparisonPredicate(
				ComparisonPredicate.GREATER_OP, new ColumnReference(
						_doubleConv, 1), new ValueSpecification(_doubleConv,
						value));

		SelectOperator selectionOrders1 = new SelectOperator(comp1);
		final List<Integer> hashLineitem = Arrays.asList(1);

		// SelectOperator selectionOrders1 = new SelectOperator(new
		// AndPredicate(comp1, comp2));

		DataSourceComponent relationOrders1 = new DataSourceComponent(
				"ORDERS1", dataPath + "orders" + extension)
				.add(selectionOrders1)
				.add(new ProjectOperator(new int[] { 0, 3 }))
				.setOutputPartKey(hashLineitem);
		_queryBuilder.add(relationOrders1);

		SelectOperator selectionOrders2 = new SelectOperator(
				new ComparisonPredicate(ComparisonPredicate.NONLESS_OP,
						new ColumnReference(_dateConv, 4),
						new ValueSpecification(_dateConv, _date1)));

		DataSourceComponent relationOrders2 = new DataSourceComponent(
				"ORDERS2", dataPath + "orders" + extension)
				.add(selectionOrders2)
				.add(new ProjectOperator(new int[] { 0, 3 }))
				.setOutputPartKey(hashLineitem);
		_queryBuilder.add(relationOrders2);

		// Aggregate
		ValueExpression<Double> substract = new Subtraction(
				new ColumnReference(_doubleConv, 1), new ColumnReference(
						_doubleConv, 3));
		AggregateOperator agg = new AggregateAvgOperator(substract, conf);

		// Join Predicate
		ColumnReference colOrders1 = new ColumnReference(_doubleConv, 1);
		ColumnReference colOrders2 = new ColumnReference(_doubleConv, 1);

		Addition add = new Addition(colOrders1, new ValueSpecification(
				_doubleConv, value));
		Subtraction sub = new Subtraction(colOrders1, new ValueSpecification(
				_doubleConv, value));
		// ComparisonPredicate pred1 = new
		// ComparisonPredicate(ComparisonPredicate.GREATER_OP, add, colOrders2);
		ComparisonPredicate pred2 = new ComparisonPredicate(
				ComparisonPredicate.LESS_OP, sub, colOrders2);
		// ComparisonPredicate pred2 = new
		// ComparisonPredicate(ComparisonPredicate.GREATER_OP, colOrders1,
		// colOrders2);

		Multiplication mult = new Multiplication(colOrders2,
				new ValueSpecification(_doubleConv, 10.0));

		ComparisonPredicate pred3 = new ComparisonPredicate(
				ComparisonPredicate.GREATER_OP, colOrders1, mult);

		Component ORDERS_ORDERSjoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationOrders1,
						relationOrders2, _queryBuilder).setJoinPredicate(pred3)
				.add(agg).setContentSensitiveThetaJoinWrapper(_doubleConv);
	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}

}