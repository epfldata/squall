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

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.OperatorComponent;
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
import ch.epfl.data.squall.types.IntegerType;
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

public class DBToasterTPCH5SequentialJoinPlan extends QueryPlan {

    private static void computeDates() {
        // date2 = date1 + 1 year
        String date1Str = "1994-01-01";
        int interval = 1;
        int unit = Calendar.YEAR;

        // setting _date1
        _date1 = _dc.fromString(date1Str);

        // setting _date2
        ValueExpression<Date> date1Ve, date2Ve;
        date1Ve = new ValueSpecification<Date>(_dc, _date1);
        date2Ve = new DateSum(date1Ve, unit, interval);
        _date2 = date2Ve.eval(null);
        // tuple is set to null since we are computing based on constants
    }

    private static Logger LOG = Logger.getLogger(DBToasterTPCH5SequentialJoinPlan.class);

    private static final IntegerType _ic = new IntegerType();
    private static final LongType _lc = new LongType();
    private static final Type<Date> _dc = new DateType();
    private static final Type<String> _sc = new StringType();

    private static final NumericType<Double> _doubleConv = new DoubleType();

    private QueryBuilder _queryBuilder = new QueryBuilder();
    // query variables
    private static Date _date1, _date2;

    private static final String REGION_NAME = "ASIA";

    public DBToasterTPCH5SequentialJoinPlan(String dataPath, String extension, Map conf) {
        computeDates();

        // -------------------------------------------------------------------------------------
        List<Integer> hashRegion = Arrays.asList(0);

        SelectOperator selectionRegion = new SelectOperator(
                new ComparisonPredicate(new ColumnReference(_sc, 1),
                        new ValueSpecification(_sc, REGION_NAME)));

        ProjectOperator projectionRegion = new ProjectOperator(new int[]{0});

        DataSourceComponent relationRegion = new DataSourceComponent("REGION",
                dataPath + "region" + extension).setOutputPartKey(hashRegion)
                .add(selectionRegion).add(projectionRegion);
        _queryBuilder.add(relationRegion);

        // -------------------------------------------------------------------------------------
        List<Integer> hashNation = Arrays.asList(2);

        ProjectOperator projectionNation = new ProjectOperator(new int[]{0,
                1, 2});

        DataSourceComponent relationNation = new DataSourceComponent("NATION",
                dataPath + "nation" + extension).setOutputPartKey(hashNation)
                .add(projectionNation);
        _queryBuilder.add(relationNation);

        // -------------------------------------------------------------------------------------
        List<Integer> hashRN = Arrays.asList(0);

        ProjectOperator projectionRN = new ProjectOperator(new int[]{0, 1});

        DBToasterJoinComponentBuilder dbtJoinBuilder = new DBToasterJoinComponentBuilder();
        dbtJoinBuilder.addRelation(relationRegion, _lc);
        dbtJoinBuilder.addRelation(relationNation, _lc, _sc, _lc);
        dbtJoinBuilder.setSQL("SELECT NATION.f0, NATION.f1 FROM REGION, NATION " +
                "WHERE NATION.f2 = REGION.f0");
        Component R_Njoin = dbtJoinBuilder.build().add(projectionRN).setOutputPartKey(hashRN);
        _queryBuilder.add(R_Njoin);
        // -------------------------------------------------------------------------------------
        List<Integer> hashSupplier = Arrays.asList(1);

        ProjectOperator projectionSupplier = new ProjectOperator(new int[]{0,
                3});

        DataSourceComponent relationSupplier = new DataSourceComponent(
                "SUPPLIER", dataPath + "supplier" + extension)
                .setOutputPartKey(hashSupplier).add(projectionSupplier);
        _queryBuilder.add(relationSupplier);

        // -------------------------------------------------------------------------------------
        List<Integer> hashRNS = Arrays.asList(2);

        ProjectOperator projectionRNS = new ProjectOperator(
                new int[]{0, 1, 2});

        dbtJoinBuilder = new DBToasterJoinComponentBuilder();
        dbtJoinBuilder.addRelation(R_Njoin, _lc, _sc);
        dbtJoinBuilder.addRelation(relationSupplier, _lc, _lc);
        dbtJoinBuilder.setSQL("SELECT REGION_NATION.f0, REGION_NATION.f1, SUPPLIER.f0 FROM " +
                "REGION_NATION, SUPPLIER WHERE REGION_NATION.f0 = SUPPLIER.f1");
        Component R_N_Sjoin = dbtJoinBuilder.build().add(projectionRNS).setOutputPartKey(hashRNS);
        _queryBuilder.add(R_N_Sjoin);
        // -------------------------------------------------------------------------------------
        List<Integer> hashLineitem = Arrays.asList(1);

        ProjectOperator projectionLineitem = new ProjectOperator(new int[]{0,
                2, 5, 6});

        DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM", dataPath + "lineitem" + extension)
                .setOutputPartKey(hashLineitem).add(projectionLineitem);
        _queryBuilder.add(relationLineitem);

        // -------------------------------------------------------------------------------------
        List<Integer> hashRNSL = Arrays.asList(0, 2);

        ProjectOperator projectionRNSL = new ProjectOperator(new int[]{0, 1,
                2, 3, 4});

        dbtJoinBuilder = new DBToasterJoinComponentBuilder();
        dbtJoinBuilder.addRelation(R_N_Sjoin, _lc, _sc, _lc);
        dbtJoinBuilder.addRelation(relationLineitem, _lc, _lc, _doubleConv, _doubleConv);
        dbtJoinBuilder.setSQL("SELECT REGION_NATION_SUPPLIER.f0, REGION_NATION_SUPPLIER.f1, LINEITEM.f0, LINEITEM.f2, LINEITEM.f3 FROM " +
                "REGION_NATION_SUPPLIER, LINEITEM WHERE REGION_NATION_SUPPLIER.f2 = LINEITEM.f1");
        Component R_N_S_Ljoin = dbtJoinBuilder.build().setOutputPartKey(hashRNSL).add(projectionRNSL);
        _queryBuilder.add(R_N_S_Ljoin);

        // -------------------------------------------------------------------------------------
        List<Integer> hashCustomer = Arrays.asList(0);

        ProjectOperator projectionCustomer = new ProjectOperator(new int[]{0,
                3});

        DataSourceComponent relationCustomer = new DataSourceComponent(
                "CUSTOMER", dataPath + "customer" + extension)
                .setOutputPartKey(hashCustomer).add(projectionCustomer);
        _queryBuilder.add(relationCustomer);

        // -------------------------------------------------------------------------------------
        List<Integer> hashOrders = Arrays.asList(1);

        SelectOperator selectionOrders = new SelectOperator(
                new BetweenPredicate(new ColumnReference(_dc, 4), true,
                        new ValueSpecification(_dc, _date1), false,
                        new ValueSpecification(_dc, _date2)));

        ProjectOperator projectionOrders = new ProjectOperator(
                new int[]{0, 1});

        DataSourceComponent relationOrders = new DataSourceComponent("ORDERS",
                dataPath + "orders" + extension).setOutputPartKey(hashOrders)
                .add(selectionOrders).add(projectionOrders);
        _queryBuilder.add(relationOrders);

        // -------------------------------------------------------------------------------------
        List<Integer> hashCO = Arrays.asList(0, 1);

        ProjectOperator projectionCO = new ProjectOperator(new int[]{0, 1});

        dbtJoinBuilder = new DBToasterJoinComponentBuilder();
        dbtJoinBuilder.addRelation(relationCustomer, _lc, _lc);
        dbtJoinBuilder.addRelation(relationOrders, _lc, _lc);
        dbtJoinBuilder.setSQL("SELECT CUSTOMER.f1, ORDERS.f0 FROM CUSTOMER, ORDERS WHERE " +
                "CUSTOMER.f0 = ORDERS.f1");
        Component C_Ojoin = dbtJoinBuilder.build().add(projectionCO).setOutputPartKey(hashCO);
        _queryBuilder.add(C_Ojoin);

        // -------------------------------------------------------------------------------------
        List<Integer> hashRNSLCO = Arrays.asList(0);

        dbtJoinBuilder = new DBToasterJoinComponentBuilder();
        // nationKey, name, orderKey, extendedPrice, discount
        dbtJoinBuilder.addRelation(R_N_S_Ljoin, _lc, _sc, _lc, _doubleConv, _doubleConv);
        // nationKey, orderKey
        dbtJoinBuilder.addRelation(C_Ojoin, _lc, _lc);
        dbtJoinBuilder.setSQL("SELECT REGION_NATION_SUPPLIER_LINEITEM.f1, SUM(REGION_NATION_SUPPLIER_LINEITEM.f3 * (1 - REGION_NATION_SUPPLIER_LINEITEM.f4)) " +
                "FROM REGION_NATION_SUPPLIER_LINEITEM, CUSTOMER_ORDERS WHERE " +
                "REGION_NATION_SUPPLIER_LINEITEM.f2 = CUSTOMER_ORDERS.f1 AND " +
                "REGION_NATION_SUPPLIER_LINEITEM.f0 = CUSTOMER_ORDERS.f0 " +
                "GROUP BY REGION_NATION_SUPPLIER_LINEITEM.f1");
        Component R_N_S_L_C_Ojoin = dbtJoinBuilder.build().setOutputPartKey(hashRNSLCO);
        R_N_S_L_C_Ojoin.setPrintOut(false);
        _queryBuilder.add(R_N_S_L_C_Ojoin);

        // -------------------------------------------------------------------------------------


        AggregateOperator agg = new AggregateSumOperator(new ColumnReference(_doubleConv, 1), conf).setGroupByColumns(0);

        OperatorComponent finalComponent = new OperatorComponent(
                R_N_S_L_C_Ojoin, "FINAL_RESULT").add(agg);
        _queryBuilder.add(finalComponent);

        // -------------------------------------------------------------------------------------
    }

    public QueryBuilder getQueryPlan() {
        return _queryBuilder;
    }
}
