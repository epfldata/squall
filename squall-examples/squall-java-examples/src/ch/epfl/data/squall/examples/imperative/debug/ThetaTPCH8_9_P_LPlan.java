/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.examples.imperative.debug;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.query_plans.ThetaQueryPlansParameters;
import ch.epfl.data.squall.types.IntegerType;
import ch.epfl.data.squall.types.StringType;

public class ThetaTPCH8_9_P_LPlan extends QueryPlan {
    private static Logger LOG = Logger.getLogger(ThetaTPCH8_9_P_LPlan.class);

    private static final StringType _sc = new StringType();
    private static final IntegerType _ic = new IntegerType();

    private QueryBuilder _queryBuilder = new QueryBuilder();

    public ThetaTPCH8_9_P_LPlan(String dataPath, String extension, Map conf) {
	int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
	// -------------------------------------------------------------------------------------
	List<Integer> hashPart = Arrays.asList(0);

	ProjectOperator projectionPart = new ProjectOperator(new int[] { 0 });

	DataSourceComponent relationPart = new DataSourceComponent("PART",
		dataPath + "part" + extension).setOutputPartKey(hashPart).add(
		projectionPart);
	_queryBuilder.add(relationPart);

	// -------------------------------------------------------------------------------------
	List<Integer> hashLineitem = Arrays.asList(1);

	ProjectOperator projectionLineitem = new ProjectOperator(new int[] { 0,
		1, 2, 4, 5, 6 });

	DataSourceComponent relationLineitem = new DataSourceComponent(
		"LINEITEM", dataPath + "lineitem" + extension)
		.setOutputPartKey(hashLineitem).add(projectionLineitem);
	_queryBuilder.add(relationLineitem);

	// -------------------------------------------------------------------------------------
	// TODO
	ColumnReference colP = new ColumnReference(_ic, 0);
	ColumnReference colL = new ColumnReference(_ic, 1);
	ComparisonPredicate P_L_comp = new ComparisonPredicate(
		ComparisonPredicate.EQUAL_OP, colP, colL);

	// AggregateCountOperator agg= new AggregateCountOperator(conf);

	Component P_Ljoin = ThetaJoinComponentFactory
		.createThetaJoinOperator(Theta_JoinType, relationPart,
			relationLineitem, _queryBuilder)
		.setOutputPartKey(Arrays.asList(0, 2))
		.setJoinPredicate(P_L_comp)
		.add(new ProjectOperator(new int[] { 0, 1, 3, 4, 5, 6 }))
		.setContentSensitiveThetaJoinWrapper(_ic)
	// .addOperator(agg)
	;

	P_Ljoin.setPrintOut(false);

	// -------------------------------------------------------------------------------------
    }

    @Override
    public QueryBuilder getQueryPlan() {
	return _queryBuilder;
    }
}
