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