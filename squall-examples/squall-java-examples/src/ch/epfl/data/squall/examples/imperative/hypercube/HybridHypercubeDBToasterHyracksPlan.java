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
import ch.epfl.data.squall.operators.AggregateSumOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.RedisOperator;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.types.LongType;
import ch.epfl.data.squall.types.StringType;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.SystemParameters;

import java.util.Arrays;
import java.util.Map;

public class HybridHypercubeDBToasterHyracksPlan extends QueryPlan {

    private final QueryBuilder _queryBuilder = new QueryBuilder();
    private static final LongType _lc = new LongType();
    private static final StringType _sc = new StringType();


    public HybridHypercubeDBToasterHyracksPlan(String dataPath, String extension, Map conf) {
        // -------------------------------------------------------------------------------------

        final ProjectOperator projectionCustomer = new ProjectOperator(
                new int[] { 0, 6 });
        final DataSourceComponent relationCustomer = new DataSourceComponent(
                "CUSTOMER", dataPath + "customer" + extension).add(
                projectionCustomer).setOutputPartKey(Arrays.asList(0));
        _queryBuilder.add(relationCustomer);


        // -------------------------------------------------------------------------------------

        final ProjectOperator projectionOrders = new ProjectOperator(
                new int[] { 0, 1 });
        final DataSourceComponent relationOrders = new DataSourceComponent(
                "ORDERS", dataPath + "orders" + extension)
                .add(projectionOrders).setOutputPartKey(Arrays.asList(1));
        _queryBuilder.add(relationOrders);

        // -------------------------------------------------------------------------------------
        DBToasterJoinComponentBuilder builder = new DBToasterJoinComponentBuilder();
        // Join keys should have the same name
        builder.addRelation(relationCustomer, new Type[]{_lc, _sc}, new String[]{"column1", "column2"});
        builder.addRelation(relationOrders, new Type[]{_lc, _lc}, new String[]{"column3", "column1"});
        
        builder.addDimension("CUSTOMER", SystemParameters.getInt(conf, "CUSTOMER_DIMENSION"), 0);
        builder.addDimension("column1", SystemParameters.getInt(conf, "column1_DIMENSION"), 1);

        builder.setSQL("SELECT CUSTOMER.f1, COUNT(ORDERS.f0) FROM CUSTOMER, ORDERS WHERE CUSTOMER.f0 = ORDERS.f1 GROUP BY CUSTOMER.f1");

        DBToasterJoinComponent dbToasterComponent = builder.build();
        dbToasterComponent.setPrintOut(false);
        _queryBuilder.add(dbToasterComponent);

        // -------------------------------------------------------------------------------------
        
        final AggregateSumOperator agg = new AggregateSumOperator(
                new ColumnReference(_lc, 1), conf).setGroupByColumns(Arrays
                .asList(0));

        OperatorComponent oc = new OperatorComponent(dbToasterComponent,
                "COUNTAGG").add(agg);
        _queryBuilder.add(oc);

        //RedisOperator redis = new RedisOperator(conf);
        //OperatorComponent pc = new OperatorComponent(oc, "SENDRESULTSTOREDIS").add(redis);

        //_queryBuilder.add(pc);

    }

    @Override
    public QueryBuilder getQueryPlan() {
        return _queryBuilder;
    }
}
