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
import ch.epfl.data.squall.components.dbtoaster.DBToasterJoinComponent;
import ch.epfl.data.squall.components.dbtoaster.DBToasterJoinComponentBuilder;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.AggregateUpdateOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.types.LongType;
import ch.epfl.data.squall.types.StringType;

import java.util.Map;

public class DBToasterHyracksPlan extends QueryPlan {

    private final QueryBuilder _queryBuilder = new QueryBuilder();
    private static final LongType _lc = new LongType();
    private static final StringType _sc = new StringType();


    public DBToasterHyracksPlan(String dataPath, String extension, Map conf) {
        // -------------------------------------------------------------------------------------
        Component relationCustomer = _queryBuilder
                .createDataSource("CUSTOMER", conf)
                .add(new ProjectOperator(0, 6)).setOutputPartKey(0);

        // -------------------------------------------------------------------------------------
        Component relationOrders = _queryBuilder
                .createDataSource("ORDERS", conf).add(new ProjectOperator(0, 1))
                .setOutputPartKey(0);

        // -------------------------------------------------------------------------------------
        DBToasterJoinComponentBuilder builder = new DBToasterJoinComponentBuilder();
        builder.addRelation(relationCustomer, new ColumnReference(_lc, 0), new ColumnReference(_sc, 1));
        builder.addRelation(relationOrders, new ColumnReference(_lc, 0), new ColumnReference(_lc, 1));
        builder.setSQL("SELECT CUSTOMER.f1, COUNT(ORDERS.f0) FROM CUSTOMER, ORDERS WHERE CUSTOMER.f0 = ORDERS.f1 GROUP BY CUSTOMER.f1");

        DBToasterJoinComponent dbToasterComponent = builder.build();

        AggregateOperator agg = new AggregateUpdateOperator(new ColumnReference(_lc, 1), conf).setGroupByColumns(0);
        dbToasterComponent.add(agg);
        _queryBuilder.add(dbToasterComponent);

    }

    @Override
    public QueryBuilder getQueryPlan() {
        return _queryBuilder;
    }
}
