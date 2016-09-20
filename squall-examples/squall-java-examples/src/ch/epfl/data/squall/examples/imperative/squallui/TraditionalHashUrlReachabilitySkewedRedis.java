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

package ch.epfl.data.squall.examples.imperative.squallui;
import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.OperatorComponent;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.hyper_cube.HyperCubeJoinComponent;
import ch.epfl.data.squall.components.hyper_cube.HyperCubeJoinComponentFactory;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.Multiplication;
import ch.epfl.data.squall.expressions.Subtraction;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.AggregateSumOperator;
import ch.epfl.data.squall.operators.AggregateCountOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.operators.RedisOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.predicates.LikePredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.query_plans.ThetaQueryPlansParameters;
import ch.epfl.data.squall.types.DateType;
import ch.epfl.data.squall.types.DoubleType;
import ch.epfl.data.squall.types.IntegerType;
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.types.LongType;
import ch.epfl.data.squall.types.StringType;
import ch.epfl.data.squall.types.Type;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class TraditionalHashUrlReachabilitySkewedRedis extends QueryPlan {

    private static Logger LOG = Logger.getLogger(TraditionalHashUrlReachabilitySkewedRedis.class);

    private static final IntegerType _ic = new IntegerType();
    private static final Type<Date> _dc = new DateType();
    private static final Type<Long> _lc = new LongType();
    private static final Type<String> _sc = new StringType();
    private static final NumericType<Double> _doubleConv = new DoubleType();

    private final QueryBuilder _queryBuilder = new QueryBuilder();


    public TraditionalHashUrlReachabilitySkewedRedis(String dataPath, String extension, Map conf) {
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
        // -------------------------------------------------------------------------------------

        HyperCubeJoinComponentFactory lastJoiner = 
            new HyperCubeJoinComponentFactory(new Component[]{
                relationArcs1, 
                relationArcs2,
                relationIndex});


        lastJoiner.addRelation(relationArcs1, 
            new Type[]{_lc, _lc}, new String[]{"From1", "To1"});
        lastJoiner.addRelation(relationArcs2,
            new Type[]{_lc, _lc}, new String[]{"To1", "From2"});
        lastJoiner.addRelation(relationIndex,
            new Type[]{_sc, _lc}, new String[]{"URL", "From1"});

        // Predicates
        ColumnReference colArcs1_To1 = new ColumnReference(_ic, 1);
        ColumnReference colArcs2_To1 = new ColumnReference(_ic, 0);
        ComparisonPredicate Arcs1_Arcs2_comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, 
            colArcs1_To1, colArcs2_To1);

        lastJoiner.addPredicate("ARCS1ARCS2", Arcs1_Arcs2_comp);

        ColumnReference colArcs1_From1 = new ColumnReference(_ic, 0);
        ColumnReference colIndex1_From1 = new ColumnReference(_ic, 1);
        ComparisonPredicate Arcs1_Index1_comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, 
            colArcs1_From1, colIndex1_From1);

        lastJoiner.addPredicate("ARCS1INDEX1", Arcs1_Index1_comp);

        final AggregateCountOperator agg = new AggregateCountOperator(conf).
            setGroupByColumns(Arrays.asList(0));

        HyperCubeJoinComponent hyper_cube = 
            lastJoiner.createHyperCubeJoinOperator().add(agg).setContentSensitiveThetaJoinWrapper(_ic);

        _queryBuilder.add(hyper_cube);
    
        //
        final AggregateSumOperator agg2 = new AggregateSumOperator(
            new ColumnReference(_lc, 1), conf).setGroupByColumns(Arrays
                .asList(0));

        OperatorComponent oc = new OperatorComponent(hyper_cube,
                "COUNTAGG").add(agg);

        _queryBuilder.add(oc);

      // Redis stuff
        RedisOperator redis = new RedisOperator(conf);
        OperatorComponent pc = new OperatorComponent(oc, "SENDRESULTSTOREDIS").add(redis);
        _queryBuilder.add(pc); 

    }

    public QueryBuilder getQueryPlan() {
        return _queryBuilder;
    }
}
