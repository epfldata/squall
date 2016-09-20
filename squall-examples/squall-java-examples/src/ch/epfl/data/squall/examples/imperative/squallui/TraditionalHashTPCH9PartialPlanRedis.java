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
import ch.epfl.data.squall.components.hyper_cube.HyperCubeJoinComponentFactory;
import ch.epfl.data.squall.components.hyper_cube.HyperCubeJoinComponent;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.Multiplication;
import ch.epfl.data.squall.expressions.Subtraction;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.AggregateSumOperator;
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

public class TraditionalHashTPCH9PartialPlanRedis extends QueryPlan {

    private static Logger LOG = Logger.getLogger(TraditionalHashTPCH9PartialPlanRedis.class);

    private static final IntegerType _ic = new IntegerType();
    private static final Type<Date> _dc = new DateType();
    private static final Type<Long> _lc = new LongType();
    private static final Type<String> _sc = new StringType();
    private static final NumericType<Double> _doubleConv = new DoubleType();
    private static final Type<Double> _double = new DoubleType();

    private final QueryBuilder _queryBuilder = new QueryBuilder();

    // query variables
    private static final String COLOR = "%green%";


    public TraditionalHashTPCH9PartialPlanRedis(String dataPath, String extension, Map conf) {
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
        // set up aggregation function on the StormComponent(Bolt) where join is
        // SUM((LINEITEM.EXTENDEDPRICE*(1.0-LINEITEM.DISCOUNT))-(PARTSUPP.SUPPLYCOST * LINEITEM.QUANTITY))
        // performed
        // 1 - discount
        ValueExpression<Double> substract = new Subtraction(
            new ValueSpecification(_doubleConv, 1.0), new ColumnReference(
                _doubleConv, 6));
         // extendedPrice*(1-discount)
        ValueExpression<Double> product = new Multiplication(
            new ColumnReference(_doubleConv, 5), substract);

        // PARTSUPP.SUPPLYCOST * LINEITEM.QUANTITY
        ValueExpression<Double> product2 = new Multiplication(
            new ColumnReference(_doubleConv, 9), new ColumnReference(_doubleConv, 4));

        // (LINEITEM.EXTENDEDPRICE*(1.0-LINEITEM.DISCOUNT))-(PARTSUPP.SUPPLYCOST * LINEITEM.QUANTITY)
        ValueExpression<Double> substract2 = new Subtraction(product, product2);
        
        AggregateOperator agg = new AggregateSumOperator(substract2, conf);

        HyperCubeJoinComponentFactory lastJoiner = 
            new HyperCubeJoinComponentFactory(new Component[]{
                relationPart, 
                relationLineitem,
                relationPartsupp});

        // PART: PARTKEY
        lastJoiner.addRelation(relationPart, new Type[]{_lc}, new String[]{"PARTKEY"});

        // LINEITEM: ORDERKEY, PARTKEY, SUPPKEY, QUANTITY, EXTENDEDPRICE, DISCOUNT
        lastJoiner.addRelation(relationLineitem, 
            new Type[]{_lc, _lc, _lc, _doubleConv, _doubleConv, _doubleConv},
            new String[]{"ORDERKEY", "PARTKEY", "SUPPKEY", "QUANTITY", "EXTENDEDPRICE", "DISCOUNT"});

        //PARTSUPP: PARTKEY, SUPPKEY, SUPPLYCOST
        lastJoiner.addRelation(relationPartsupp, 
            new Type[]{_lc, _lc, _doubleConv},
            new String[]{"PARTKEY", "SUPPKEY", "SUPPLYCOST"}); 

        // Predicates
        ColumnReference colP_PARTKEY = new ColumnReference(_ic, 0);
        ColumnReference colL_PARTKEY = new ColumnReference(_ic, 1);
        ComparisonPredicate P_L_comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, 
            colP_PARTKEY, colL_PARTKEY);

        lastJoiner.addPredicate("PARTLINEITEM", P_L_comp);

        ColumnReference colL_SUPPKEY = new ColumnReference(_ic, 2);
        ColumnReference colPS_SUPPKEY = new ColumnReference(_ic, 0);
        ComparisonPredicate L_PS_comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, 
            colL_SUPPKEY, colPS_SUPPKEY);

        lastJoiner.addPredicate("LINEITEMPARTSUPP", L_PS_comp);

        HyperCubeJoinComponent hypercubeComponent = lastJoiner.createHyperCubeJoinOperator().add(agg);
        _queryBuilder.add(hypercubeComponent);

        // Redis stuff
        AggregateOperator finalAgg = new AggregateSumOperator(new ColumnReference(_double, 0), conf);
        OperatorComponent finalComponent = new OperatorComponent(hypercubeComponent, "FINAL_RESULT").add(finalAgg);
        _queryBuilder.add(finalComponent);

        RedisOperator redis = new RedisOperator(conf);
        OperatorComponent pc = new OperatorComponent(finalComponent, "SENDRESULTSTOREDIS").add(redis);
        _queryBuilder.add(pc);
    }

    public QueryBuilder getQueryPlan() {
        return _queryBuilder;
    }
}
