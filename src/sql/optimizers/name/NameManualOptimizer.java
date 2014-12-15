package sql.optimizers.name;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import plan_runner.components.Component;
import plan_runner.query_plans.QueryBuilder;
import plan_runner.utilities.SystemParameters;
import sql.optimizers.Optimizer;
import sql.util.ParserUtil;
import sql.visitors.jsql.SQLVisitor;

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
		final int totalParallelism = SystemParameters.getInt(_map, "DIP_TOTAL_SRC_PAR");
		final NameCompGenFactory factory = new NameCompGenFactory(_map, _pq.getTan(),
				totalParallelism);
		final NameCompGen ncg = factory.create();

		Component first = ncg.generateDataSource(_compNames.get(0));
		for (int i = 1; i < _compNames.size(); i++) {
			final Component second = ncg.generateDataSource(_compNames.get(i));
			first = ncg.generateEquiJoin(first, second);
		}

		ParserUtil.parallelismToMap(ncg, _map);

		return ncg.getQueryPlan();
	}

	// HELPER methods
	private void parse() {
		final String plan = SystemParameters.getString(_map, "DIP_PLAN");
		final String[] components = plan.split(",");

		_compNames.addAll(Arrays.asList(components));
	}

}