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


package ch.epfl.data.squall.examples.imperative;

import java.util.ArrayList;
import java.util.Map;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.EquiJoinComponent;
import ch.epfl.data.squall.components.OperatorComponent;
import ch.epfl.data.squall.operators.AggregateCountOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.storm_components.StormEmitter;
import ch.epfl.data.squall.storm_components.StormOperator;

public class SimpleAggregationPlan extends QueryPlan {

	public SimpleAggregationPlan(String dataPath, String extension, Map conf) {
          super(dataPath, extension, conf);
	}

	@Override
	public Component createQueryPlan(String dataPath, String extension, Map conf) {
		// -------------------------------------------------------------------------------------
		Component sorted1 = new DataSourceComponent("SORTED-1", conf).setOutputPartKey(new int[] { 0 });
		Component sorted2 = new DataSourceComponent("SORTED-2", conf).setOutputPartKey(new int[] { 0 });
		Component sorted3 = new DataSourceComponent("SORTED-3", conf).setOutputPartKey(new int[] { 0 });
		
//		Component sorted1 = new DataSourceComponent("RANDOM-1", conf).setOutputPartKey(new int[] { 0 });
//		Component sorted2 = new DataSourceComponent("RANDOM-2", conf).setOutputPartKey(new int[] { 0 });
//		Component sorted = new DataSourceComponent("RANDOM-3", conf).setOutputPartKey(new int[] { 0 });
		
		ArrayList<Component> arr = new ArrayList<Component>();
		arr.add(sorted1);arr.add(sorted2);arr.add(sorted3);
//		Component sorted = new DataSourceComponent("RANDOM", conf).setOutputPartKey(new int[] { 0 });

		OperatorComponent op = new OperatorComponent(arr, "COUNT")
		.add(new AggregateCountOperator(conf).setGroupByColumns(0));
		
		// -------------------------------------------------------------------------------------

		return op;
		// -------------------------------------------------------------------------------------
	}
}
