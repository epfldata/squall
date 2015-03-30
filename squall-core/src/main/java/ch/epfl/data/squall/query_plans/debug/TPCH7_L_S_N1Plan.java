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


package ch.epfl.data.squall.query_plans.debug;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.EquiJoinComponent;
import ch.epfl.data.squall.conversion.DateConversion;
import ch.epfl.data.squall.conversion.DoubleConversion;
import ch.epfl.data.squall.conversion.IntegerConversion;
import ch.epfl.data.squall.conversion.NumericConversion;
import ch.epfl.data.squall.conversion.StringConversion;
import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.IntegerYearFromDate;
import ch.epfl.data.squall.expressions.Multiplication;
import ch.epfl.data.squall.expressions.Subtraction;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.BetweenPredicate;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.predicates.OrPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;

public class TPCH7_L_S_N1Plan extends QueryPlan {
	private static Logger LOG = Logger.getLogger(TPCH7_L_S_N1Plan.class);

	private QueryBuilder _queryBuilder = new QueryBuilder();

	private static final String _date1Str = "1995-01-01";
	private static final String _date2Str = "1996-12-31";
	// private static final String _firstCountryName = "FRANCE";
	// private static final String _secondCountryName = "GERMANY";
	private static final String _firstCountryName = "PERU";
	private static final String _secondCountryName = "ETHIOPIA";

	private static final IntegerConversion _ic = new IntegerConversion();
	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final TypeConversion<String> _sc = new StringConversion();
	private static final Date _date1 = _dateConv.fromString(_date1Str);
	private static final Date _date2 = _dateConv.fromString(_date2Str);

	public TPCH7_L_S_N1Plan(String dataPath, String extension, Map conf) {
		// -------------------------------------------------------------------------------------
		List<Integer> hashSupplier = Arrays.asList(1);

		ProjectOperator projectionSupplier = new ProjectOperator(new int[] { 0,
				3 });

		DataSourceComponent relationSupplier = new DataSourceComponent(
				"SUPPLIER", dataPath + "supplier" + extension)
				.setOutputPartKey(hashSupplier).add(projectionSupplier);
		_queryBuilder.add(relationSupplier);

		// -------------------------------------------------------------------------------------
		List<Integer> hashNation1 = Arrays.asList(1);

		SelectOperator selectionNation2 = new SelectOperator(new OrPredicate(
				new ComparisonPredicate(new ColumnReference(_sc, 1),
						new ValueSpecification(_sc, _firstCountryName)),
				new ComparisonPredicate(new ColumnReference(_sc, 1),
						new ValueSpecification(_sc, _secondCountryName))));

		ProjectOperator projectionNation1 = new ProjectOperator(new int[] { 1,
				0 });

		DataSourceComponent relationNation1 = new DataSourceComponent(
				"NATION1", dataPath + "nation" + extension)
				.setOutputPartKey(hashNation1).add(selectionNation2)
				.add(projectionNation1);
		_queryBuilder.add(relationNation1);

		// -------------------------------------------------------------------------------------

		ColumnReference colS = new ColumnReference(_ic, 1);
		ColumnReference colN = new ColumnReference(_ic, 1);
		ComparisonPredicate S_N_pred = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colS, colN);

		EquiJoinComponent S_Njoin = new EquiJoinComponent(relationSupplier,
				relationNation1).add(new ProjectOperator(new int[] { 0, 2 }))
				.setJoinPredicate(S_N_pred).setOutputPartKey(Arrays.asList(0));
		_queryBuilder.add(S_Njoin);

		// -------------------------------------------------------------------------------------
		List<Integer> hashLineitem = Arrays.asList(2);

		SelectOperator selectionLineitem = new SelectOperator(
				new BetweenPredicate(new ColumnReference(_dateConv, 10), true,
						new ValueSpecification(_dateConv, _date1), true,
						new ValueSpecification(_dateConv, _date2)));

		// first field in projection
		ValueExpression extractYear = new IntegerYearFromDate(
				new ColumnReference<Date>(_dateConv, 10));
		// second field in projection
		// 1 - discount
		ValueExpression<Double> substract = new Subtraction(
				new ValueSpecification(_doubleConv, 1.0), new ColumnReference(
						_doubleConv, 6));
		// extendedPrice*(1-discount)
		ValueExpression<Double> product = new Multiplication(
				new ColumnReference(_doubleConv, 5), substract);
		// third field in projection
		ColumnReference supplierKey = new ColumnReference(_sc, 2);
		// forth field in projection
		ColumnReference orderKey = new ColumnReference(_sc, 0);
		ProjectOperator projectionLineitem = new ProjectOperator(extractYear,
				product, supplierKey, orderKey);

		DataSourceComponent relationLineitem = new DataSourceComponent(
				"LINEITEM", dataPath + "lineitem" + extension)
				.setOutputPartKey(hashLineitem).add(selectionLineitem)
				.add(projectionLineitem);
		_queryBuilder.add(relationLineitem);

		// -------------------------------------------------------------------------------------
		/*
		 * // AggregateCountOperator agg= new AggregateCountOperator(conf);
		 * EquiJoinComponent L_S_Njoin = new EquiJoinComponent(
		 * relationLineitem, S_Njoin, _queryPlan).addOperator(new
		 * ProjectOperator(new int[]{4, 0, 1, 3}))
		 * .setHashIndexes(Arrays.asList(3)) // .addOperator(agg) ;
		 * L_S_Njoin.setPrintOut(false);
		 */

		ColumnReference colL = new ColumnReference(_ic, 2);
		ColumnReference colS_N = new ColumnReference(_ic, 0);
		ComparisonPredicate L_S_N_pred = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colL, colS_N);

		EquiJoinComponent L_S_Njoin = new EquiJoinComponent(relationLineitem,
				S_Njoin).add(new ProjectOperator(new int[] { 5, 0, 1, 3 }))
				.setOutputPartKey(Arrays.asList(3))
				.setJoinPredicate(L_S_N_pred);
		_queryBuilder.add(L_S_Njoin);
		L_S_Njoin.setPrintOut(false);

		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
