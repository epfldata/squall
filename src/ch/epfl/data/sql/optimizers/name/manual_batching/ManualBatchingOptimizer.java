package ch.epfl.data.sql.optimizers.name.manual_batching;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import ch.epfl.data.plan_runner.utilities.SystemParameters;
import ch.epfl.data.sql.optimizers.Optimizer;
import ch.epfl.data.sql.optimizers.name.CostParams;
import ch.epfl.data.sql.optimizers.name.NameCompGen;
import ch.epfl.data.sql.util.ImproperParallelismException;
import ch.epfl.data.sql.util.ParserUtil;
import ch.epfl.data.sql.visitors.jsql.SQLVisitor;

/*
 * For left-deep plans, parallelism obtained from cost formula
 */
public class ManualBatchingOptimizer implements Optimizer {
	private final SQLVisitor _pq;
	private final Map _map;

	private static Logger LOG = Logger.getLogger(ManualBatchingOptimizer.class);

	public ManualBatchingOptimizer(Map map) {
		_map = map;
		_pq = ParserUtil.parseQuery(map);
	}

	private void addEquiJoinNotSuboptimal(Component firstComp,
			Component secondComp, NameCompGen ncg, List<NameCompGen> listNcg) {

		boolean isExc = false;
		try {
			ncg.generateEquiJoin(firstComp, secondComp);
		} catch (final ImproperParallelismException exc) {
			final StringBuilder errorMsg = new StringBuilder();
			errorMsg.append(
					"This subplan will never generated the optimal query plan, so it's thrown:")
					.append("\n");
			errorMsg.append(exc.getMessage()).append("\n");
			LOG.info(errorMsg.toString());
			isExc = true;
		}
		if (!isExc)
			// if this subplan is somewhat suboptimal, it's not added to the
			// list
			listNcg.add(ncg);
	}

	/*
	 * best is the one with the smallest total nodes used
	 */
	private NameCompGen chooseBest(List<NameCompGen> ncgList) {
		if (ncgList.isEmpty()) {
			final String errorMsg = "No query plans can be efficiently executed with specified parallelisms.\n"
					+ "Try to reduce DIP_TOTAL_SRC_PAR in config file.";
			LOG.info(errorMsg);
			System.exit(1);
		}

		final int index = getMinTotalLatencyIndex(ncgList);
		return ncgList.get(index);
	}

	@Override
	public QueryBuilder generate() {
		final int totalSourcePar = SystemParameters.getInt(_map,
				"DIP_TOTAL_SRC_PAR");
		final ManualBatchingCompGenFactory factory = new ManualBatchingCompGenFactory(
				_map, _pq.getTan(), totalSourcePar);
		final List<String> sourceNames = factory.getParAssigner()
				.getSortedSourceNames();
		final int numSources = sourceNames.size();
		NameCompGen optimal = null;

		// **************creating single-relation plans********************
		if (numSources == 1) {
			optimal = factory.create();
			optimal.generateDataSource(sourceNames.get(0));
		}

		// **************creating 2-way joins********************
		List<NameCompGen> ncgListFirst = new ArrayList<NameCompGen>();
		for (int i = 0; i < numSources; i++) {
			final String firstCompName = sourceNames.get(i);
			final List<String> joinedWith = _pq.getJte()
					.getJoinedWithSingleDir(firstCompName);
			if (joinedWith != null)
				for (final String secondCompName : joinedWith) {
					final NameCompGen ncg = factory.create();
					final Component first = ncg
							.generateDataSource(firstCompName);
					final Component second = ncg
							.generateDataSource(secondCompName);
					addEquiJoinNotSuboptimal(first, second, ncg, ncgListFirst);
				}
		}
		if (numSources == 2)
			optimal = chooseBest(ncgListFirst);

		// **************creating multi-way joins********************
		for (int level = 2; level < numSources; level++) {
			final List<NameCompGen> ncgListSecond = new ArrayList<NameCompGen>();
			for (int i = 0; i < ncgListFirst.size(); i++) {
				final NameCompGen ncg = ncgListFirst.get(i);
				Component firstComp = ncg.getQueryBuilder().getLastComponent();
				final List<String> ancestors = ParserUtil
						.getSourceNameList(firstComp);
				final List<String> joinedWith = _pq.getJte().getJoinedWith(
						ancestors);
				for (final String compName : joinedWith) {
					NameCompGen newNcg = ncg;
					if (joinedWith.size() > 1) {
						// doing deepCopy only if there are multiple tables to
						// be joined with
						newNcg = ncg.deepCopy();
						firstComp = newNcg.getQueryBuilder().getLastComponent();
					}

					final Component secondComp = newNcg
							.generateDataSource(compName);
					addEquiJoinNotSuboptimal(firstComp, secondComp, newNcg,
							ncgListSecond);
				}
			}

			if (level == numSources - 1)
				// last level, chooseOptimal
				optimal = chooseBest(ncgListSecond);
			else {
				// filtering - for NCGs with the same ancestor set, choose the
				// one with the smallest totalParallelism
				// ncgListSecond = pruneSubplans(ncgListSecond);
			}

			ncgListFirst = ncgListSecond;
		}

		ParserUtil.parallelismToMap(optimal, _map);
		ParserUtil.batchesToMap(optimal, _map);

		LOG.info("Predicted latency is " + getTotalLatency(optimal));
		return optimal.getQueryBuilder();
	}

	private int getMinTotalLatencyIndex(List<NameCompGen> ncgList) {
		double totalLatency = getTotalLatency(ncgList.get(0));
		int minLatencyIndex = 0;
		for (int i = 1; i < ncgList.size(); i++) {
			final double currentTotalLatency = getTotalLatency(ncgList.get(i));
			if (currentTotalLatency < totalLatency) {
				minLatencyIndex = i;
				totalLatency = currentTotalLatency;
			}
		}
		return minLatencyIndex;
	}

	// TODO: should compare them by parallelism as well, and not only by
	// totalLatency
	// we could also do some pruning
	private double getTotalLatency(NameCompGen ncg) {
		final Map<String, CostParams> allParams = ncg.getCompCost();
		final Component lastComponent = ncg.getQueryBuilder()
				.getLastComponent();
		final CostParams lastParams = allParams.get(lastComponent.getName());
		return lastParams.getTotalAvgLatency(); // it's computed as query plan
		// is built on
	}

}
