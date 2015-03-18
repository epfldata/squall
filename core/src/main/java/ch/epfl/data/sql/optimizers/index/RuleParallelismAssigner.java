package ch.epfl.data.sql.optimizers.index;

import java.util.List;
import java.util.Map;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import ch.epfl.data.plan_runner.utilities.SystemParameters;
import ch.epfl.data.sql.schema.Schema;
import ch.epfl.data.sql.util.TableAliasName;

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