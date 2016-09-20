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

package ch.epfl.data.squall.examples.imperative.hypercube;

import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.OperatorComponent;
import ch.epfl.data.squall.components.dbtoaster.DBToasterJoinComponent;
import ch.epfl.data.squall.components.dbtoaster.DBToasterJoinComponentBuilder;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.DateSum;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.expressions.IntegerYearFromDate;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.AggregateSumOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.BetweenPredicate;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.predicates.LikePredicate;
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

public class HybridHypercubeDBToasterTPCH9Plan extends QueryPlan {

    private static Logger LOG = Logger.getLogger(HybridHypercubeDBToasterTPCH9Plan.class);

    private static final Type<Date> _dc = new DateType();
    private static final Type<Long> _lc = new LongType();
    private static final Type<String> _sc = new StringType();
    private static final NumericType<Double> _doubleConv = new DoubleType();

    private final QueryBuilder _queryBuilder = new QueryBuilder();

    // query variables
    private static final String COLOR = "%green%";


    public HybridHypercubeDBToasterTPCH9Plan(String dataPath, String extension, Map conf) {
        // -------------------------------------------------------------------------------------
        List<Integer> hashPart = Arrays.asList(0);

        SelectOperator selectionPart = new SelectOperator(
            new LikePredicate(new ColumnReference(_sc, 1),
                new ValueSpecification(_sc, COLOR)));

        ProjectOperator projectionPart = new ProjectOperator(new int[] { 0 });

        DataSourceComponent relationPart = new DataSourceComponent("PART",
            dataPath + "part" + extension, conf).setOutputPartKey(hashPart)
            .add(selectionPart).add(projectionPart);
        _queryBuilder.add(relationPart);


        // -------------------------------------------------------------------------------------
        final List<Integer> hashLineitem = Arrays.asList(1);

        final ProjectOperator projectionLineitem = new ProjectOperator(
                new int[] { 0, 1, 2, 4, 5, 6 });

        final DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM", dataPath + "lineitem" + extension, conf)
                .setOutputPartKey(hashLineitem).add(projectionLineitem);
        _queryBuilder.add(relationLineitem); 

        // -------------------------------------------------------------------------------------

        List<Integer> hashPartsupp = Arrays.asList(0, 1);

        ProjectOperator projectionPartsupp = new ProjectOperator(new int[] { 0,
            1, 3 });

        DataSourceComponent relationPartsupp = new DataSourceComponent(
            "PARTSUPP", dataPath + "partsupp" + extension, conf)
            .setOutputPartKey(hashPartsupp).add(projectionPartsupp);
        _queryBuilder.add(relationPartsupp);

        // -------------------------------------------------------------------------------------

        List<Integer> hashOrders = Arrays.asList(0);

        ProjectOperator projectionOrders = new ProjectOperator(
            new ColumnReference(_sc, 0), new IntegerYearFromDate(
                new ColumnReference(_dc, 4)));

        DataSourceComponent relationOrders = new DataSourceComponent("ORDERS",
            dataPath + "orders" + extension, conf).setOutputPartKey(hashOrders)
            .add(projectionOrders);
        _queryBuilder.add(relationOrders);


        // -------------------------------------------------------------------------------------
        List<Integer> hashSupplier = Arrays.asList(0);

        ProjectOperator projectionSupplier = new ProjectOperator(new int[] { 0,
            3 });

        DataSourceComponent relationSupplier = new DataSourceComponent(
            "SUPPLIER", dataPath + "supplier" + extension, conf)
            .setOutputPartKey(hashSupplier).add(projectionSupplier);
        _queryBuilder.add(relationSupplier);

        // -------------------------------------------------------------------------------------
        List<Integer> hashNation = Arrays.asList(0);

        ProjectOperator projectionNation = new ProjectOperator(
            new int[] { 0, 1 });

        DataSourceComponent relationNation = new DataSourceComponent("NATION",
            dataPath + "nation" + extension, conf).setOutputPartKey(hashNation)
            .add(projectionNation);
        _queryBuilder.add(relationNation);

        // -------------------------------------------------------------------------------------
        DBToasterJoinComponentBuilder dbtBuilder = new DBToasterJoinComponentBuilder();
        // PART: PARTKEY
        dbtBuilder.addRelation(relationPart, 
            new Type[]{_lc},
            new String[]{"PARTKEY"});

        // LINEITEM: ORDERKEY, PARTKEY, SUPPKEY, QUANTITY, EXTENDEDPRICE, DISCOUNT
        dbtBuilder.addRelation(relationLineitem,
            new Type[]{_lc, _lc, _lc, _doubleConv, _doubleConv, _doubleConv},
            new String[]{"L_ORDERKEY", "PARTKEY", "SUPPKEY", "QUANTITY", "EXTENDEDPRICE", "DISCOUNT"},
            new int[]{0});

        //PARTSUPP: PARTKEY, SUPPKEY, SUPPLYCOST
        dbtBuilder.addRelation(relationPartsupp, 
            new Type[]{_lc, _lc, _doubleConv},
            new String[]{"PARTKEY", "SUPPKEY", "SUPPLYCOST"}); 
   
        //ORDERS: ORDERKEY, EXTRACT_YEAR(ORDERS.ORDERDATE)
        dbtBuilder.addRelation(relationOrders, 
            new Type[]{_lc, _lc},
            new String[]{"O_ORDERKEY", "ORDERDATE"},
            new int[]{0});

        //SUPPLIER: SUPPKEY, NATIONKEY
        dbtBuilder.addRelation(relationSupplier, 
            new Type[]{_lc, _lc},
            new String[]{"SUPPKEY", "NATIONKEY"});

        //NATION: NATIONKEY, NAME
        dbtBuilder.addRelation(relationNation, 
            new Type[]{_lc, _sc},
            new String[]{"NATIONKEY", "NAME"});

        dbtBuilder.setSQL("SELECT NATION.f1, ORDERS.f1, " + 
                "SUM((LINEITEM.f4 * (1.0 - LINEITEM.f5))-(PARTSUPP.f2 * LINEITEM.f3)) " +
                "FROM PART, LINEITEM, PARTSUPP, ORDERS, SUPPLIER, NATION " +
                "WHERE PART.f0 = LINEITEM.f1 AND " +
                "PARTSUPP.f0 = LINEITEM.f1 AND PARTSUPP.f1 = LINEITEM.f2 AND " +
                "LINEITEM.f0 = ORDERS.f0 AND SUPPLIER.f0 = LINEITEM.f2 AND " +
                "NATION.f0 = SUPPLIER.f1 " +
                "GROUP BY NATION.f1, ORDERS.f1");

        DBToasterJoinComponent dbtComp = dbtBuilder.build();
        dbtComp.setPrintOut(false);
        _queryBuilder.add(dbtComp);        

    }

    public QueryBuilder getQueryPlan() {
        return _queryBuilder;
    }
}
