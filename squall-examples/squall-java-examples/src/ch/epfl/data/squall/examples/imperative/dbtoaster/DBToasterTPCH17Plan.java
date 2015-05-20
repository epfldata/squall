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
import ch.epfl.data.squall.components.dbtoaster.DBToasterJoinComponent;
import ch.epfl.data.squall.components.dbtoaster.DBToasterJoinComponentBuilder;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.types.DoubleType;
import ch.epfl.data.squall.types.LongType;
import ch.epfl.data.squall.types.MultiplicityType;
import ch.epfl.data.squall.types.StringType;
import ch.epfl.data.squall.types.Type;

import java.util.Map;

/*
 SELECT SUM(L_EXTENDEDPRICE)/7.0 AS AVG_YEARLY FROM LINEITEM, PART
WHERE P_PARTKEY = L_PARTKEY AND P_BRAND = 'Brand#44' AND P_CONTAINER = 'WRAP PKG'
AND L_QUANTITY < (SELECT 0.2*AVG(L_QUANTITY) FROM LINEITEM WHERE L_PARTKEY = P_PARTKEY)
 */
public class DBToasterTPCH17Plan extends QueryPlan {

    private final QueryBuilder _queryBuilder = new QueryBuilder();
    private static final Type<Long> _long = new LongType();
    private static final Type<Double> _double = new DoubleType();
    private static final Type<String> _string = new StringType();
    private static final Type _multiplicity = new MultiplicityType();
    private static final String P_BRAND = "Brand#44";
    private static final String P_CONTAINER = "WRAP PKG";


    public DBToasterTPCH17Plan(String dataPath, String extension, Map conf) {

        final ProjectOperator projectionLineitem = new ProjectOperator(
                new int[] { 1, 4 });

        final DataSourceComponent relationLineItem = new DataSourceComponent(
                "LINEITEM", dataPath + "lineitem" + extension)
                .setOutputPartKey(0).add(projectionLineitem);
        _queryBuilder.add(relationLineItem);

        //----------------------------------------------------------------------------

        DBToasterJoinComponentBuilder builder = new DBToasterJoinComponentBuilder();
        builder.addRelation(relationLineItem, _long, _long);
        builder.setComponentName("L_AVG");
        builder.setSQL("SELECT LINEITEM.f1, 0.2 * AVG(LINEITEM.f0) FROM LINEITEM GROUP BY LINEITEM.f1");
        Component nestedL_avg = builder.build()
                                                .setOutputWithMultiplicity(true)
                                                .setOutputPartKey(0);
        _queryBuilder.add(nestedL_avg);

        //----------------------------------------------------------------------------

        final ProjectOperator projectionOuterLineitem = new ProjectOperator(
                new int[] {1, 4, 5}); // partkey, quantity, extended price

        final DataSourceComponent outerRelationLineItem = new DataSourceComponent(
                "LINEITEM", dataPath + "lineitem" + extension)
                .setOutputPartKey(0).add(projectionOuterLineitem);
        _queryBuilder.add(outerRelationLineItem);

        // ---------------------------------------------------------------------------

        final SelectOperator selectionPBrand = new SelectOperator(
                new ComparisonPredicate(new ColumnReference(_string, 3),
                        new ValueSpecification(_string, P_BRAND)));

        final SelectOperator selectionPContainer = new SelectOperator(
                new ComparisonPredicate(new ColumnReference(_string, 6),
                        new ValueSpecification(_string, P_CONTAINER)));

        final ProjectOperator projectionPart = new ProjectOperator(
                new int[] {0}); // partkey, brand, container

        final DataSourceComponent relationPart = new DataSourceComponent(
                "PART", dataPath + "part" + extension)
                .setOutputPartKey(0)
                .add(selectionPBrand)
                .add(selectionPContainer)
                .add(projectionPart);
        _queryBuilder.add(relationPart);

        // ------------------------------------------------------------------

        builder = new DBToasterJoinComponentBuilder();
        builder.addRelation(nestedL_avg, _multiplicity, _long, _double); // partkey, avg_quantity
        builder.addRelation(relationPart, _long);
        builder.addRelation(outerRelationLineItem, _long, _long, _double); // partkey, quantity, extended price

        builder.setSQL("SELECT SUM(LINEITEM.f2)/7.0 AS AVG_YEARLY FROM LINEITEM, PART WHERE " +
                "PART.f0 = LINEITEM.f0 AND " +
                "PART.f0 = L_AVG.f");

    }

    public QueryBuilder getQueryPlan() {
        return _queryBuilder;
    }
}
