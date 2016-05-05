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
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.types.DateType;
import ch.epfl.data.squall.types.IntegerType;
import ch.epfl.data.squall.types.Type;

public class HyracksL1SelectDatePlan extends QueryPlan {
    private static Logger LOG = Logger.getLogger(HyracksL1SelectDatePlan.class);

    private final QueryBuilder _queryBuilder = new QueryBuilder();

    private static final IntegerType _ic = new IntegerType();
    private static final Type<Date> _dc = new DateType();
    private static final Date _dateConst = _dc.fromString("1954-01-01");
    
    public HyracksL1SelectDatePlan(String dataPath, String extension, Map conf) {
	// -------------------------------------------------------------------------------------
	// start of query plan filling
	// the selections are no-ops
	final SelectOperator selCustomer = new SelectOperator(
		new ComparisonPredicate(ComparisonPredicate.GREATER_OP,
			new ColumnReference(_ic, 0), new ValueSpecification(_ic, -1)));
	final SelectOperator selOrders = new SelectOperator(
		new ComparisonPredicate(ComparisonPredicate.GREATER_OP,
			new ColumnReference(_dc, 4), new ValueSpecification(_dc, _dateConst)));
	
	
	final ProjectOperator projectionCustomer = new ProjectOperator(0, 6);
	final List<Integer> hashCustomer = Arrays.asList(0);
	DataSourceComponent relationCustomer = new DataSourceComponent("customer", conf)
		.add(selCustomer)
		.add(projectionCustomer)
		.setOutputPartKey(hashCustomer)
		.setPrintOut(false);
	_queryBuilder.add(relationCustomer);

	// -------------------------------------------------------------------------------------
	final ProjectOperator projectionOrders = new ProjectOperator(1);
	final List<Integer> hashOrders = Arrays.asList(0);
	DataSourceComponent relationOrders = new DataSourceComponent("orders", conf)
		.add(selOrders)
		.add(projectionOrders)
		.setOutputPartKey(hashOrders)
		.setPrintOut(false);
	_queryBuilder.add(relationOrders);

    }

    @Override
    public QueryBuilder getQueryPlan() {
	return _queryBuilder;
    }

}