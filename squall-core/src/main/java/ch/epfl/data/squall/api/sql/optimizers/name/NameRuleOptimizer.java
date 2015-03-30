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


package ch.epfl.data.squall.api.sql.optimizers.name;

import java.util.List;
import java.util.Map;

import ch.epfl.data.squall.api.sql.optimizers.Optimizer;
import ch.epfl.data.squall.api.sql.util.ParserUtil;
import ch.epfl.data.squall.api.sql.visitors.jsql.SQLVisitor;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.utilities.SystemParameters;

/*
 * For lefty plans, parallelism obtained from cost formula
 */
public class NameRuleOptimizer implements Optimizer {
	private final Map _map;
	private final SQLVisitor _pq;

	public NameRuleOptimizer(Map map) {
		_map = map;
		_pq = ParserUtil.parseQuery(map);
	}

	/*
	 * Take last component(LC) from ncg, and remove the Source with the smallest
	 * cardinality which can be joined with LC. This method has side effects:
	 * removing from sourceNames collection
	 */
	private String chooseSmallestSource(Component lastComp,
			List<String> sourceNames) {
		for (int i = 0; i < sourceNames.size(); i++) {
			final String candidateComp = sourceNames.get(i);
			if (_pq.getJte().joinExistsBetween(candidateComp,
					ParserUtil.getSourceNameList(lastComp))) {
				sourceNames.remove(i);
				return candidateComp;
			}
		}
		throw new RuntimeException(
				"Should not be here! No components to join with!");
	}

	@Override
	public QueryBuilder generate() {
		final int totalParallelism = SystemParameters.getInt(_map,
				"DIP_TOTAL_SRC_PAR");
		final NameCompGenFactory factory = new NameCompGenFactory(_map,
				_pq.getTan(), totalParallelism);

		// sorted by increasing cardinalities
		final List<String> sourceNames = factory.getParAssigner()
				.getSortedSourceNames();

		final NameCompGen ncg = factory.create();

		Component first = ncg.generateDataSource(sourceNames.remove(0));
		final int numSources = sourceNames.size(); // first component is already
		// removed
		for (int i = 0; i < numSources; i++) {
			final String secondStr = chooseSmallestSource(first, sourceNames);
			final Component second = ncg.generateDataSource(secondStr);
			first = ncg.generateEquiJoin(first, second);
		}

		ParserUtil.parallelismToMap(ncg, _map);

		return ncg.getQueryBuilder();
	}

}