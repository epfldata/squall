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
import ch.epfl.data.squall.expressions.DateSum;
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
import ch.epfl.data.squall.types.LongType;
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.types.StringType;
import ch.epfl.data.squall.types.Type;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class HybridHypercubeDBToasterTPCH5Plan extends QueryPlan {

    private static Logger LOG = Logger.getLogger(HybridHypercubeDBToasterTPCH5Plan.class);

    private static final Type<Date> _dc = new DateType();
    private static final Type<Long> _lc = new LongType();
    private static final Type<String> _sc = new StringType();
    private static final NumericType<Double> _doubleConv = new DoubleType();

    private final QueryBuilder _queryBuilder = new QueryBuilder();

    // query variables
    private static Date _date1, _date2;
    private static final String REGION_NAME = "ASIA";

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

    public HybridHypercubeDBToasterTPCH5Plan(String dataPath, String extension, Map conf) {
        computeDates();

        // -------------------------------------------------------------------------------------
        final List<Integer> hashRegion = Arrays.asList(0);

        final SelectOperator selectionRegion = new SelectOperator(
                new ComparisonPredicate(new ColumnReference(_sc, 1),
                        new ValueSpecification(_sc, REGION_NAME)));

        final ProjectOperator projectionRegion = new ProjectOperator(
                new int[] { 0 });

        final DataSourceComponent relationRegion = new DataSourceComponent(
                "REGION", dataPath + "region" + extension, conf)
                .setOutputPartKey(hashRegion).add(selectionRegion)
                .add(projectionRegion);
        _queryBuilder.add(relationRegion);

        // -------------------------------------------------------------------------------------
        final List<Integer> hashNation = Arrays.asList(2);

        final ProjectOperator projectionNation = new ProjectOperator(new int[] {
                0, 1, 2 });

        final DataSourceComponent relationNation = new DataSourceComponent(
                "NATION", dataPath + "nation" + extension, conf).setOutputPartKey(
                hashNation).add(projectionNation);
        _queryBuilder.add(relationNation);

        // -------------------------------------------------------------------------------------

        final List<Integer> hashSupplier = Arrays.asList(1);

        final ProjectOperator projectionSupplier = new ProjectOperator(
                new int[] { 0, 3 });

        final DataSourceComponent relationSupplier = new DataSourceComponent(
                "SUPPLIER", dataPath + "supplier" + extension, conf)
                .setOutputPartKey(hashSupplier).add(projectionSupplier);
        _queryBuilder.add(relationSupplier);

        // -------------------------------------------------------------------------------------
        final List<Integer> hashLineitem = Arrays.asList(1);

        final ProjectOperator projectionLineitem = new ProjectOperator(
                new int[] { 0, 2, 5, 6 });

        final DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM", dataPath + "lineitem" + extension, conf)
                .setOutputPartKey(hashLineitem).add(projectionLineitem);
        _queryBuilder.add(relationLineitem);

        // -------------------------------------------------------------------------------------
        final List<Integer> hashCustomer = Arrays.asList(0);

        final ProjectOperator projectionCustomer = new ProjectOperator(
                new int[] { 0, 3 });

        final DataSourceComponent relationCustomer = new DataSourceComponent(
                "CUSTOMER", dataPath + "customer" + extension, conf)
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
                "ORDERS", dataPath + "orders" + extension, conf)
                .setOutputPartKey(hashOrders).add(selectionOrders)
                .add(projectionOrders);
        _queryBuilder.add(relationOrders);

        // -------------------------------------------------------------------------------------

        DBToasterJoinComponentBuilder dbtBuilder = new DBToasterJoinComponentBuilder();
        // Region: regionKey
        dbtBuilder.addRelation(relationRegion, new Type[]{_lc}, new String[]{"regionKey"});
        // Nation: nationKey, name, regionKey
        dbtBuilder.addRelation(relationNation, new Type[]{_lc, _sc, _lc}, 
            new String[]{"nationKey", "name", "regionKey"});
        // Supplier: supkey, nationKey
        dbtBuilder.addRelation(relationSupplier, new Type[]{_lc, _lc}, 
            new String[]{"supkey", "nationKey"});
        // Lineitem: orderKey, supKey, extendedPrice, discount
        dbtBuilder.addRelation(relationLineitem, new Type[]{_lc, _lc, _doubleConv, _doubleConv}, 
            new String[]{"orderKey", "supKey", "extendedPrice", "discount"});
        // Customer: custKey, nationKey
        dbtBuilder.addRelation(relationCustomer, new Type[]{_lc, _lc}, 
            new String[]{"custKey", "nationKey"});
        // Orders: orderKey, custKey
        dbtBuilder.addRelation(relationOrders, new Type[]{_lc, _lc}, 
            new String[]{"orderKey", "custKey"});

        dbtBuilder.setSQL("SELECT NATION.f1, SUM(LINEITEM.f2 * (1 - LINEITEM.f3)) " +
                "FROM CUSTOMER, ORDERS, LINEITEM, SUPPLIER, NATION, REGION " +
                "WHERE CUSTOMER.f0 = ORDERS.f1 AND LINEITEM.f0 = ORDERS.f0 AND LINEITEM.f1 = SUPPLIER.f0 " +
                "AND CUSTOMER.f1 = SUPPLIER.f1 AND SUPPLIER.f1 = NATION.f0 AND NATION.f2 = REGION.f0 " +
                "GROUP BY NATION.f1");

        DBToasterJoinComponent dbtComp = dbtBuilder.build();
        dbtComp.setPrintOut(false);
        _queryBuilder.add(dbtComp);


        AggregateOperator agg = new AggregateSumOperator(new ColumnReference(_doubleConv, 1), conf).setGroupByColumns(0);

        OperatorComponent finalComponent = new OperatorComponent(
                dbtComp, "FINAL_RESULT").add(agg);
        _queryBuilder.add(finalComponent);

    }

    public QueryBuilder getQueryPlan() {
        return _queryBuilder;
    }
}
