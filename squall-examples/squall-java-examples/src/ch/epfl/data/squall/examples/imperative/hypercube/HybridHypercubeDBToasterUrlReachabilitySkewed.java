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
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.AggregateSumOperator;
import ch.epfl.data.squall.operators.CustomSampleOperatorReachGraph;
import ch.epfl.data.squall.operators.SampleOperator;
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

public class HybridHypercubeDBToasterUrlReachabilitySkewed extends QueryPlan {
    private static Logger LOG = Logger.getLogger(HybridHypercubeDBToasterUrlReachabilitySkewed.class);

    private static final Type<String> _sc = new StringType();
    private static final Type<Long> _lc = new LongType();
    private static final Type<Integer> _ic = new IntegerType();
 
    private final QueryBuilder _queryBuilder = new QueryBuilder();

    public HybridHypercubeDBToasterUrlReachabilitySkewed(String dataPath, String extension, Map conf) {

        // -------------------------------------------------------------------------------------
        // columns : From -> To
        // final CustomSampleOperatorReachGraph samples1 = new CustomSampleOperatorReachGraph(30, true);
        final SelectOperator selectionIndegree = new SelectOperator(
            new ComparisonPredicate(new ColumnReference(_sc, 1),
                new ValueSpecification(_sc, "5325333")));

        final DataSourceComponent relationArcs1 = new DataSourceComponent(
                "ARCS1", dataPath + "pld-arc" + extension, conf).add(selectionIndegree);
        _queryBuilder.add(relationArcs1);


        // -------------------------------------------------------------------------------------
        // columns : From -> To
        // final CustomSampleOperatorReachGraph samples2 = new CustomSampleOperatorReachGraph(30, false);
        final SelectOperator selectionOutdegree = new SelectOperator(
            new ComparisonPredicate(new ColumnReference(_sc, 0),
                new ValueSpecification(_sc, "5325333")));

        final DataSourceComponent relationArcs2 = new DataSourceComponent(
                "ARCS2", dataPath + "pld-arc" + extension, conf).add(selectionOutdegree);
        _queryBuilder.add(relationArcs2);


        // // -------------------------------------------------------------------------------------
        //final SampleOperator samples3 = new SampleOperator(0.25);
        final DataSourceComponent relationIndex = new DataSourceComponent(
                "INDEX1", dataPath + "pld-index" + extension, conf);//.add(samples3);
        _queryBuilder.add(relationIndex);


        // -----------------------------------------------------------------------------------
        DBToasterJoinComponentBuilder dbToasterCompBuilder = new DBToasterJoinComponentBuilder();
        dbToasterCompBuilder.addRelation(relationArcs1, 
            new Type[]{_lc, _lc}, new String[]{"From1", "To1_rand1"}, new int[]{1});
        dbToasterCompBuilder.addRelation(relationArcs2,
            new Type[]{_lc, _lc}, new String[]{"To1_rand2", "From2"}, new int[]{0});
        dbToasterCompBuilder.addRelation(relationIndex,
            new Type[]{_sc, _lc}, new String[]{"URL", "From1"});

        dbToasterCompBuilder.setSQL("SELECT ARCS1.f1, COUNT(*) " +
                "FROM ARCS1, ARCS2, INDEX1 " +
                "WHERE ARCS1.f1 = ARCS2.f0 AND ARCS1.f0 = INDEX1.f1 " + 
                "GROUP BY ARCS1.f1 ");

        DBToasterJoinComponent dbToasterComponent = dbToasterCompBuilder.build();
        dbToasterComponent.setPrintOut(false);

        _queryBuilder.add(dbToasterComponent);

        final AggregateSumOperator agg = new AggregateSumOperator(
                new ColumnReference(_lc, 1), conf).setGroupByColumns(Arrays
                .asList(0));;

        OperatorComponent oc = new OperatorComponent(dbToasterComponent,
                "COUNTAGG").add(agg);
        _queryBuilder.add(oc);
    }

    public QueryBuilder getQueryPlan() {
        return _queryBuilder;
    }
}
