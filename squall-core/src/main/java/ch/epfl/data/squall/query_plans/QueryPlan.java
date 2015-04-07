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


package ch.epfl.data.squall.query_plans;

import java.util.Map;

import ch.epfl.data.squall.components.Component;

public abstract class QueryPlan {
	private final QueryBuilder _queryBuilder = new QueryBuilder();

	public QueryPlan(String dataPath, String extension, Map conf) {
          build(createQueryPlan(dataPath, extension, conf));
	}

	public QueryPlan() { }

	// _queryBuilder expects components in the parent->child order
	// root is the leaf child
	protected void build(Component root) {
		if (root == null)
			return;
		Component[] parents = root.getParents();
		if (parents != null) {
			for (Component parent : parents) {
				build(parent);
			}
		}
		_queryBuilder.add(root);
	}

	// This returns the last component: it assumes there is only one last
	// component
	// TODO: put it back to abstract and update plans
	//public abstract Component createQueryPlan(String dataPath, String extension, Map conf);
	public Component createQueryPlan(String dataPath, String extension, Map conf) {
		return null;
	}

	// QueryBuilder is expected from the outside world
	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
