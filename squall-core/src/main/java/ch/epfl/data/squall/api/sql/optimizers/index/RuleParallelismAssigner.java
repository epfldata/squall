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

package ch.epfl.data.squall.api.sql.optimizers.index;

import java.util.List;
import java.util.Map;

import ch.epfl.data.squall.api.sql.schema.Schema;
import ch.epfl.data.squall.api.sql.util.TableAliasName;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.utilities.SystemParameters;

public class RuleParallelismAssigner {
    private final int THRESHOLD_TUPLES = 100; // both nation and region has less
    // number of tuples

    private final QueryBuilder _plan;
    private final int _maxInputPar;
    private final TableAliasName _tan;
    private final Schema _schema;
    private final Map _map;

    public RuleParallelismAssigner(QueryBuilder plan, TableAliasName tan,
	    Schema schema, Map map) {
	_plan = plan;
	_tan = tan;
	_schema = schema;
	_map = map;
	_maxInputPar = SystemParameters.getInt(map, "DIP_MAX_SRC_PAR");
    }

    public void assignPar() {
	final LevelAssigner topDown = new LevelAssigner(
		_plan.getLastComponent());
	final List<DataSourceComponent> dsList = topDown.getSources();
	final List<CompLevel> clList = topDown.getNonSourceComponents();

	assignParDataSource(dsList);
	assignParNonDataSource(clList);
    }

    private void assignParDataSource(List<DataSourceComponent> sources) {
	for (final DataSourceComponent source : sources) {
	    final String compName = source.getName();
	    final String compMapStr = compName + "_PAR";
	    if (getNumOfTuples(compName) > THRESHOLD_TUPLES)
		SystemParameters.putInMap(_map, compMapStr, _maxInputPar);
	    else
		SystemParameters.putInMap(_map, compMapStr, 1);
	}
    }

    private void assignParNonDataSource(List<CompLevel> clList) {
	for (final CompLevel cl : clList) {
	    final Component comp = cl.getComponent();
	    final String compName = comp.getName();
	    final String compMapStr = compName + "_PAR";
	    int level = cl.getLevel();

	    if (comp.getParents().length < 2)
		// an operatorComponent should have no more parallelism than its
		// (only) parent
		level--;

	    // TODO: for the last operatorComponent, parallelism should be based
	    // on groupBy

	    int parallelism = (int) (_maxInputPar * Math.pow(2, level - 2));
	    if (parallelism < 1)
		// cannot be less than 1
		parallelism = 1;

	    SystemParameters.putInMap(_map, compMapStr, parallelism);
	}
    }

    private long getNumOfTuples(String compName) {
	final String schemaName = _tan.getSchemaName(compName);
	return _schema.getTableSize(schemaName);
    }
}