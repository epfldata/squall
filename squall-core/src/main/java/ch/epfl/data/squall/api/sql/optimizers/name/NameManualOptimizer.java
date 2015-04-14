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

import java.util.ArrayList;
import java.util.Arrays;
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
public class NameManualOptimizer implements Optimizer {
    private final Map _map;
    private final SQLVisitor _pq;

    private final List<String> _compNames = new ArrayList<String>(); // all the

    // sources in
    // the
    // appropriate
    // order

    public NameManualOptimizer(Map map) {
	_map = map;
	_pq = ParserUtil.parseQuery(map);

	parse();
    }

    @Override
    public QueryBuilder generate() {
	final int totalParallelism = SystemParameters.getInt(_map,
		"DIP_TOTAL_SRC_PAR");
	final NameCompGenFactory factory = new NameCompGenFactory(_map,
		_pq.getTan(), totalParallelism);
	final NameCompGen ncg = factory.create();

	Component first = ncg.generateDataSource(_compNames.get(0));
	for (int i = 1; i < _compNames.size(); i++) {
	    final Component second = ncg.generateDataSource(_compNames.get(i));
	    first = ncg.generateEquiJoin(first, second);
	}

	ParserUtil.parallelismToMap(ncg, _map);

	return ncg.getQueryBuilder();
    }

    // HELPER methods
    private void parse() {
	final String plan = SystemParameters.getString(_map, "DIP_PLAN");
	final String[] components = plan.split(",");

	_compNames.addAll(Arrays.asList(components));
    }

}