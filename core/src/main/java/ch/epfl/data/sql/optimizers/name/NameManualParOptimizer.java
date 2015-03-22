package ch.epfl.data.sql.optimizers.name;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import ch.epfl.data.plan_runner.utilities.SystemParameters;
import ch.epfl.data.sql.optimizers.Optimizer;
import ch.epfl.data.sql.util.ParserUtil;
import ch.epfl.data.sql.visitors.jsql.SQLVisitor;

/*
 * For lefty plans with explicitly specified parallelism
 */
public class NameManualParOptimizer implements Optimizer {
	private final Map _map;
	private final SQLVisitor _pq;

	private final List<String> _compNames = new ArrayList<String>(); // all the
	// sources in
	// the
	// appropriate
	// order
	private final Map<String, Integer> _compNamePar = new HashMap<String, Integer>(); // all

	// components(including
	// joins)
	// with
	// its
	// parallelism

	public NameManualParOptimizer(Map map) {
		_map = map;
		_pq = ParserUtil.parseQuery(map);

		try {
			parse();
		} catch (final ArrayIndexOutOfBoundsException a) {
			throw new RuntimeException(
					"Invalid DIP_PLAN setting in config file!");
		}
	}

	@Override
	public QueryBuilder generate() {
		final NameCompGenFactory factory = new NameCompGenFactory(_map,
				_pq.getTan());
		final NameCompGen ncg = factory.create();

		Component first = ncg.generateDataSource(_compNames.get(0));
		for (int i = 1; i < _compNames.size(); i++) {
			final Component second = ncg.generateDataSource(_compNames.get(i));
			first = ncg.generateEquiJoin(first, second);
		}

		ParserUtil.parallelismToMap(_compNamePar, _map);

		return ncg.getQueryBuilder();
	}

	// HELPER methods
	private void parse() {
		final String plan = SystemParameters.getString(_map, "DIP_PLAN");
		final String[] components = plan.split(",");

		final String firstComponent = components[0];
		final String[] firstParts = firstComponent.split(":");
		String firstCompName = firstParts[0];
		final int firstParallelism = Integer.valueOf(firstParts[1]);

		putSource(firstCompName, firstParallelism);

		for (int i = 1; i < components.length; i++) {
			final String secondComponent = components[i];
			final String[] secondParts = secondComponent.split(":");
			final String secondCompName = secondParts[0];
			final int secondPar = Integer.valueOf(secondParts[1]);
			final int secondJoinPar = Integer.valueOf(secondParts[2]);
			putSource(secondCompName, secondPar);
			final String joinCompName = firstCompName + "_" + secondCompName;
			putJoin(joinCompName, secondJoinPar);
			firstCompName = joinCompName;
		}
	}

	private void putJoin(String compName, int par) {
		_compNamePar.put(compName, par);
	}

	private void putSource(String compName, int par) {
		_compNames.add(compName);
		_compNamePar.put(compName, par);
	}

}
