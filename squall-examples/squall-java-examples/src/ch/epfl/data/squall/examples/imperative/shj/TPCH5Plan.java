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

import ch.epfl.data.squall.predicates.AndPredicate;
import ch.epfl.data.squall.types.IntegerType;
import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.EquiJoinComponent;
import ch.epfl.data.squall.components.OperatorComponent;
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

public class TPCH5Plan extends QueryPlan {
    private static void computeDates() {
	// date2 = date1 + 1 year
	final String date1Str = "1994-01-01";
	final int interval = 1;
	final int unit = Calendar.YEAR;

	// setting _date1
	_date1 = _dc.fromString(date1Str);

	// setting _date2
	ValueExpression<Date> date1Ve, date2Ve;
	date1Ve = new ValueSpecification<Date>(_dc, _date1);
	date2Ve = new DateSum(date1Ve, unit, interval);
	_date2 = date2Ve.eval(null);
	// tuple is set to null since we are computing based on constants
    }

    private static Logger LOG = Logger.getLogger(TPCH5Plan.class);
    private static final IntegerType _ic = new IntegerType();

    private static final Type<Date> _dc = new DateType();
    private static final Type<String> _sc = new StringType();

    private static final NumericType<Double> _doubleConv = new DoubleType();

    private final QueryBuilder _queryBuilder = new QueryBuilder();
    // query variables
    private static Date _date1, _date2;

    private static final String REGION_NAME = "ASIA";

    public TPCH5Plan(String dataPath, String extension, Map conf) {
	computeDates();

	// -------------------------------------------------------------------------------------
	final List<Integer> hashRegion = Arrays.asList(0);

	final SelectOperator selectionRegion = new SelectOperator(
		new ComparisonPredicate(new ColumnReference(_sc, 1),
			new ValueSpecification(_sc, REGION_NAME)));

	final ProjectOperator projectionRegion = new ProjectOperator(
		new int[] { 0 });

	final DataSourceComponent relationRegion = new DataSourceComponent(
		"REGION", dataPath + "region" + extension)
		.setOutputPartKey(hashRegion).add(selectionRegion)
		.add(projectionRegion);
	_queryBuilder.add(relationRegion);

	// -------------------------------------------------------------------------------------
	final List<Integer> hashNation = Arrays.asList(2);

	final ProjectOperator projectionNation = new ProjectOperator(new int[] {
		0, 1, 2 });

	final DataSourceComponent relationNation = new DataSourceComponent(
		"NATION", dataPath + "nation" + extension).setOutputPartKey(
		hashNation).add(projectionNation);
	_queryBuilder.add(relationNation);

	// -------------------------------------------------------------------------------------
	final List<Integer> hashRN = Arrays.asList(0);

	final ProjectOperator projectionRN = new ProjectOperator(new int[] { 1,
		2 });

    ColumnReference colR = new ColumnReference(_ic, 0);
    ColumnReference colN = new ColumnReference(_ic, 2);
    ComparisonPredicate R_N_comp = new ComparisonPredicate(
            ComparisonPredicate.EQUAL_OP, colR, colN);

	final EquiJoinComponent R_Njoin = new EquiJoinComponent(relationRegion,
		relationNation).setOutputPartKey(hashRN).add(projectionRN).setJoinPredicate(R_N_comp);
	_queryBuilder.add(R_Njoin);

	// -------------------------------------------------------------------------------------
	final List<Integer> hashSupplier = Arrays.asList(1);

	final ProjectOperator projectionSupplier = new ProjectOperator(
		new int[] { 0, 3 });

	final DataSourceComponent relationSupplier = new DataSourceComponent(
		"SUPPLIER", dataPath + "supplier" + extension)
		.setOutputPartKey(hashSupplier).add(projectionSupplier);
	_queryBuilder.add(relationSupplier);

	// -------------------------------------------------------------------------------------
	final List<Integer> hashRNS = Arrays.asList(2);

	final ProjectOperator projectionRNS = new ProjectOperator(new int[] {
		0, 1, 2 });

    ColumnReference colR_N = new ColumnReference(_ic, 0);
    ColumnReference colS = new ColumnReference(_ic, 1);
    ComparisonPredicate R_N_S_comp = new ComparisonPredicate(
            ComparisonPredicate.EQUAL_OP, colR_N, colS);

	final EquiJoinComponent R_N_Sjoin = new EquiJoinComponent(R_Njoin,
		relationSupplier).setOutputPartKey(hashRNS).add(projectionRNS).setJoinPredicate(R_N_S_comp);
	_queryBuilder.add(R_N_Sjoin);

	// -------------------------------------------------------------------------------------
	final List<Integer> hashLineitem = Arrays.asList(1);

	final ProjectOperator projectionLineitem = new ProjectOperator(
		new int[] { 0, 2, 5, 6 });

	final DataSourceComponent relationLineitem = new DataSourceComponent(
		"LINEITEM", dataPath + "lineitem" + extension)
		.setOutputPartKey(hashLineitem).add(projectionLineitem);
	_queryBuilder.add(relationLineitem);

	// -------------------------------------------------------------------------------------
	final List<Integer> hashRNSL = Arrays.asList(0, 2);

	final ProjectOperator projectionRNSL = new ProjectOperator(new int[] {
		0, 1, 3, 5, 6 });

    ColumnReference colR_N_S = new ColumnReference(_ic, 2);
    ColumnReference colL = new ColumnReference(_ic, 1);
    ComparisonPredicate R_N_S_L_comp = new ComparisonPredicate(
            ComparisonPredicate.EQUAL_OP, colR_N_S, colL);

	final EquiJoinComponent R_N_S_Ljoin = new EquiJoinComponent(R_N_Sjoin,
		relationLineitem).setOutputPartKey(hashRNSL)
		.add(projectionRNSL)
        .setJoinPredicate(R_N_S_L_comp);
	_queryBuilder.add(R_N_S_Ljoin);

	// -------------------------------------------------------------------------------------
	final List<Integer> hashCustomer = Arrays.asList(0);

	final ProjectOperator projectionCustomer = new ProjectOperator(
		new int[] { 0, 3 });

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
	final List<Integer> hashCO = Arrays.asList(0, 1);

	final ProjectOperator projectionCO = new ProjectOperator(new int[] { 1,
		2 });

    ColumnReference colC = new ColumnReference(_ic, 0);
    ColumnReference colO = new ColumnReference(_ic, 1);
    ComparisonPredicate C_O_comp = new ComparisonPredicate(
            ComparisonPredicate.EQUAL_OP, colC, colO);
	final EquiJoinComponent C_Ojoin = new EquiJoinComponent(
		relationCustomer, relationOrders).setOutputPartKey(hashCO).add(
		projectionCO).setJoinPredicate(C_O_comp);
	_queryBuilder.add(C_Ojoin);

	// -------------------------------------------------------------------------------------
	final List<Integer> hashRNSLCO = Arrays.asList(0);

	final ProjectOperator projectionRNSLCO = new ProjectOperator(new int[] {
		1, 3, 4 });

    ColumnReference colR_N_S_L1 = new ColumnReference(_ic, 0);
    ColumnReference colR_N_S_L2 = new ColumnReference(_ic, 2);
    ColumnReference colC_O1 = new ColumnReference(_ic, 0);
    ColumnReference colC_O2 = new ColumnReference(_ic, 1);
    ComparisonPredicate pred1 = new ComparisonPredicate(
            ComparisonPredicate.EQUAL_OP, colR_N_S_L1, colC_O1);
    ComparisonPredicate pred2 = new ComparisonPredicate(
            ComparisonPredicate.EQUAL_OP, colR_N_S_L2, colC_O2);
    AndPredicate R_N_S_L_C_O_comp = new AndPredicate(pred1, pred2);

	final EquiJoinComponent R_N_S_L_C_Ojoin = new EquiJoinComponent(
		R_N_S_Ljoin, C_Ojoin).setOutputPartKey(hashRNSLCO).add(
		projectionRNSLCO).setJoinPredicate(R_N_S_L_C_O_comp);
	_queryBuilder.add(R_N_S_L_C_Ojoin);

	// -------------------------------------------------------------------------------------
	// set up aggregation function on a separate StormComponent(Bolt)

	final ValueExpression<Double> substract = new Subtraction(
		new ValueSpecification(_doubleConv, 1.0), new ColumnReference(
			_doubleConv, 2));
	// extendedPrice*(1-discount)
	final ValueExpression<Double> product = new Multiplication(
		new ColumnReference(_doubleConv, 1), substract);

	final AggregateOperator aggOp = new AggregateSumOperator(product, conf)
		.setGroupByColumns(Arrays.asList(0));
	OperatorComponent oc = new OperatorComponent(R_N_S_L_C_Ojoin,
		"FINAL_RESULT").add(aggOp);
	_queryBuilder.add(oc);

	// -------------------------------------------------------------------------------------
    }

    @Override
    public QueryBuilder getQueryPlan() {
	return _queryBuilder;
    }
}
