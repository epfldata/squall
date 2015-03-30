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


package ch.epfl.data.squall.query_plans.debug;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.squall.conversion.IntegerConversion;
import ch.epfl.data.squall.conversion.StringConversion;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.AndPredicate;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.query_plans.theta.ThetaQueryPlansParameters;
import ch.epfl.data.squall.storm_components.InterchangingComponent;
import ch.epfl.data.squall.utilities.SystemParameters;

public class ThetaOrdersLineFluctuationsPlan extends QueryPlan {

	private QueryBuilder _queryBuilder = new QueryBuilder();
	private static final IntegerConversion _ic = new IntegerConversion();
	private static final StringConversion _sc = new StringConversion();

	public ThetaOrdersLineFluctuationsPlan(String dataPath, String extension,
			Map conf) {

		int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);

		/**
		 * Multiplicative Factor
		 */
		int multFactor = SystemParameters.getInt(conf, "DIP_MULT_FACTOR");

		AndPredicate and = new AndPredicate(new ComparisonPredicate(
				ComparisonPredicate.NONEQUAL_OP, new ColumnReference(_sc, 5),
				new ValueSpecification(_sc, "5-LOW")), new ComparisonPredicate(
				ComparisonPredicate.NONEQUAL_OP, new ColumnReference(_sc, 5),
				new ValueSpecification(_sc, "1-URGENT")));
		SelectOperator selectionOrders = new SelectOperator(and);
		final List<Integer> hashLineitem = Arrays.asList(0);

		// Orders
		ProjectOperator projectionOrders = new ProjectOperator(new int[] { 0 });
		DataSourceComponent relationOrders = new DataSourceComponent("ORDERS",
				dataPath + "orders" + extension).add(selectionOrders)
				.add(projectionOrders).setOutputPartKey(hashLineitem);
		_queryBuilder.add(relationOrders);

		// Lineitem
		ProjectOperator projectionLineitem = new ProjectOperator(
				new int[] { 0 });
		DataSourceComponent relationLineitem = new DataSourceComponent(
				"LINEITEM", dataPath + "lineitem" + extension).add(
				projectionLineitem).setOutputPartKey(hashLineitem);
		_queryBuilder.add(relationLineitem);

		InterchangingComponent interComp = new InterchangingComponent(
				relationOrders, relationLineitem, multFactor);
		_queryBuilder.add(interComp);

		// ///////////////I KNOW NOT CLEAN :D
		// this would not work on the cluster
		// ThetaReshufflerAdvisedEpochsNew.isInterchanging=true;
		// ///////////////////////

		ColumnReference colO = new ColumnReference(_ic, 0);
		ColumnReference colL = new ColumnReference(_ic, 0);
		ComparisonPredicate O_L_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colO, colL);

		Component join = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationOrders,
						relationLineitem, _queryBuilder)
				.setInterComp(interComp).setJoinPredicate(O_L_comp)
				.setContentSensitiveThetaJoinWrapper(_ic);

		join.setPrintOut(false);

		/*
		 * Component LINEITEMS_LINEITEMSjoin = null; if(Theta_JoinType==0){
		 * LINEITEMS_LINEITEMSjoin = new ThetaJoinStaticComponent(
		 * relationLineitem1, relationLineitem2,
		 * _queryPlan).setJoinPredicate(pred5) // .addOperator(agg) ; } else
		 * if(Theta_JoinType==1){ LINEITEMS_LINEITEMSjoin = new
		 * ThetaJoinDynamicComponentNaiive( relationLineitem1,
		 * relationLineitem2, _queryPlan,
		 * ThetaQueryPlansParameters.getThetaDynamicRefreshRate
		 * (conf)).setJoinPredicate(pred5) // .addOperator(agg) ; } else
		 * if(Theta_JoinType==2){ LINEITEMS_LINEITEMSjoin = new
		 * ThetaJoinDynamicComponentAdvised( relationLineitem1,
		 * relationLineitem2, _queryPlan,
		 * ThetaQueryPlansParameters.getThetaDynamicRefreshRate
		 * (conf)).setJoinPredicate(pred5) // .addOperator(agg) ; } else
		 * if(Theta_JoinType==3){ LINEITEMS_LINEITEMSjoin = new
		 * ThetaJoinDynamicComponentAdvisedEpochs( relationLineitem1,
		 * relationLineitem2, _queryPlan).setJoinPredicate(pred5) //
		 * .addOperator(agg) ; } LINEITEMS_LINEITEMSjoin.setPrintOut(false);
		 */

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}

}
