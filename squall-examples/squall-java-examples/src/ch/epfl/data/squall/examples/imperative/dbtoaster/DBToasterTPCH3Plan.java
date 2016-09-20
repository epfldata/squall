/*
 *
 *  * Copyright (c) 2011-2015 EPFL DATA Laboratory
 *  * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *  *
 *  * All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package ch.epfl.data.squall.examples.imperative.dbtoaster;

import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.OperatorComponent;
import ch.epfl.data.squall.components.dbtoaster.DBToasterJoinComponent;
import ch.epfl.data.squall.components.dbtoaster.DBToasterJoinComponentBuilder;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.AggregateSumOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.types.DateLongType;
import ch.epfl.data.squall.types.DateType;
import ch.epfl.data.squall.types.DoubleType;
import ch.epfl.data.squall.types.IntegerType;
import ch.epfl.data.squall.types.LongType;
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.types.StringType;
import ch.epfl.data.squall.types.Type;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class DBToasterTPCH3Plan extends QueryPlan {
    private static Logger LOG = Logger.getLogger(DBToasterTPCH3Plan.class);

    private static final String _customerMktSegment = "BUILDING";
    private static final String _dateStr = "1995-03-15";

    private static final Type<Date> _dateConv = new DateType();
    private static final Type<Long> _dateLongConv = new DateLongType();
    private static final NumericType<Double> _doubleConv = new DoubleType();
    private static final Type<String> _sc = new StringType();
    private static final Type<Long> _lc = new LongType();
    private static final Type<Integer> _ic = new IntegerType();
    private static final Date _date = _dateConv.fromString(_dateStr);

    private final QueryBuilder _queryBuilder = new QueryBuilder();

    public DBToasterTPCH3Plan(String dataPath, String extension, Map conf) {

        // -------------------------------------------------------------------------------------
        final List<Integer> hashCustomer = Arrays.asList(0);

        final SelectOperator selectionCustomer = new SelectOperator(
                new ComparisonPredicate(new ColumnReference(_sc, 6),
                        new ValueSpecification(_sc, _customerMktSegment)));

        final ProjectOperator projectionCustomer = new ProjectOperator(
                new int[]{0});

        final DataSourceComponent relationCustomer = new DataSourceComponent(
                "CUSTOMER", dataPath + "customer" + extension, conf)
                .setOutputPartKey(hashCustomer).add(selectionCustomer)
                .add(projectionCustomer);
        _queryBuilder.add(relationCustomer);

        // -------------------------------------------------------------------------------------
        final List<Integer> hashOrders = Arrays.asList(1);

        final SelectOperator selectionOrders = new SelectOperator(
                new ComparisonPredicate(ComparisonPredicate.LESS_OP,
                        new ColumnReference(_dateConv, 4),
                        new ValueSpecification(_dateConv, _date)));

        final ProjectOperator projectionOrders = new ProjectOperator(new int[]{
                0, 1, 4, 7});

        final DataSourceComponent relationOrders = new DataSourceComponent(
                "ORDERS", dataPath + "orders" + extension, conf)
                .setOutputPartKey(hashOrders).add(selectionOrders)
                .add(projectionOrders);
        _queryBuilder.add(relationOrders);

        // -------------------------------------------------------------------------------------
        final List<Integer> hashLineitem = Arrays.asList(0);

        final SelectOperator selectionLineitem = new SelectOperator(
                new ComparisonPredicate(ComparisonPredicate.GREATER_OP,
                        new ColumnReference(_dateConv, 10),
                        new ValueSpecification(_dateConv, _date)));

        final ProjectOperator projectionLineitem = new ProjectOperator(
                new int[]{0, 5, 6});

        final DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM", dataPath + "lineitem" + extension, conf)
                .setOutputPartKey(hashLineitem).add(selectionLineitem)
                .add(projectionLineitem);
        _queryBuilder.add(relationLineitem);

        // -----------------------------------------------------------------------------------
        DBToasterJoinComponentBuilder dbToasterCompBuilder = new DBToasterJoinComponentBuilder();
        dbToasterCompBuilder.addRelation(relationCustomer, _lc);
        dbToasterCompBuilder.addRelation(relationOrders, _lc, _lc,
                _sc, _lc); // Have to use DateLongConversion instead of DateConversion as DBToaster use Long as Date
        dbToasterCompBuilder.addRelation(relationLineitem, _lc, _doubleConv,
                _doubleConv);
        dbToasterCompBuilder.setSQL("SELECT LINEITEM.f0, SUM(LINEITEM.f1 * (1 - LINEITEM.f2)) FROM CUSTOMER, ORDERS, LINEITEM " +
                "WHERE CUSTOMER.f0 = ORDERS.f1 AND ORDERS.f0 = LINEITEM.f0 " +
                "GROUP BY LINEITEM.f0, ORDERS.f2, ORDERS.f3");

        DBToasterJoinComponent dbToasterComponent = dbToasterCompBuilder.build();
        dbToasterComponent.setPrintOut(false);

        _queryBuilder.add(dbToasterComponent);


        final AggregateSumOperator agg = new AggregateSumOperator(
                new ColumnReference(_doubleConv, 3), conf).setGroupByColumns(Arrays
                .asList(0, 1, 2));

        OperatorComponent oc = new OperatorComponent(dbToasterComponent,
                "COUNTAGG").add(agg);
        _queryBuilder.add(oc);

    }

    public QueryBuilder getQueryPlan() {
        return _queryBuilder;
    }
}
