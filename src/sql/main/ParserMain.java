package sql.main;

import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.main.Main;
import plan_runner.query_plans.QueryPlan;
import plan_runner.utilities.SystemParameters;
import sql.optimizers.Optimizer;
import sql.optimizers.index.IndexRuleOptimizer;
import sql.optimizers.index.IndexSimpleOptimizer;
import sql.optimizers.name.NameCostOptimizer;
import sql.optimizers.name.NameManualOptimizer;
import sql.optimizers.name.NameManualParOptimizer;
import sql.optimizers.name.NameRuleOptimizer;
import sql.optimizers.name.manual_batching.ManualBatchingOptimizer;
import sql.util.ParserUtil;

public class ParserMain {
	private static Logger LOG = Logger.getLogger(ParserMain.class);

	public static void main(String[] args) {
		final String parserConfPath = args[0];
		final ParserMain pm = new ParserMain();

		Map map = pm.createConfig(parserConfPath);
		// map has to filled before plan is created
		final QueryPlan plan = pm.generatePlan(map);
		// we have to set ackers after we know how many workers are there(which
		// is done in generatePlan)
		map = pm.putAckers(plan, map);

		LOG.info(ParserUtil.toString(plan));
		LOG.info(ParserUtil.parToString(plan, map));

		new Main(plan, map, parserConfPath);
	}

	// String[] sizes: {"1G", "2G", "4G", ...}
	public Map createConfig(String parserConfPath) {
		final Map map = SystemParameters.fileToMap(parserConfPath);

		final String dbSize = SystemParameters.getString(map, "DIP_DB_SIZE") + "G";
		final String dataRoot = SystemParameters.getString(map, "DIP_DATA_ROOT");
		final String dataPath = dataRoot + "/" + dbSize + "/";

		SystemParameters.putInMap(map, "DIP_DATA_PATH", dataPath);

		return map;
	}

	public QueryPlan generatePlan(Map map) {
		final Optimizer opt = pickOptimizer(map);
		return opt.generate();
	}

	private Optimizer pickOptimizer(Map map) {
		final String optStr = SystemParameters.getString(map, "DIP_OPTIMIZER_TYPE");
		LOG.info("Selected optimizer: " + optStr);
		if ("INDEX_SIMPLE".equalsIgnoreCase(optStr))
			// Simple optimizer provides lefty plans
			return new IndexSimpleOptimizer(map);
		else if ("INDEX_RULE_BUSHY".equalsIgnoreCase(optStr))
			return new IndexRuleOptimizer(map);
		else if ("NAME_MANUAL_PAR_LEFTY".equalsIgnoreCase(optStr))
			return new NameManualParOptimizer(map);
		else if ("NAME_MANUAL_COST_LEFTY".equalsIgnoreCase(optStr))
			return new NameManualOptimizer(map);
		else if ("NAME_RULE_LEFTY".equalsIgnoreCase(optStr))
			return new NameRuleOptimizer(map);
		else if ("NAME_COST_LEFTY".equalsIgnoreCase(optStr))
			return new NameCostOptimizer(map);
		else if ("NAME_MANUAL_BATCHING".equalsIgnoreCase(optStr))
			return new ManualBatchingOptimizer(map);
		throw new RuntimeException("Unknown " + optStr + " optimizer!");
	}

	private Map putAckers(QueryPlan plan, Map map) {
		final int numWorkers = ParserUtil.getTotalParallelism(plan, map);
		int localAckers, clusterAckers;

		if (!SystemParameters.getBoolean(map, "DIP_ACK_EVERY_TUPLE")) {
			// we don't ack after each tuple is sent,
			// so we don't need any node to be dedicated for acking
			localAckers = 0;
			clusterAckers = 0;
		} else {
			// on local machine we always set it to 1, because there is no so
			// many cores
			localAckers = 1;

			// this is a heuristic which could be changed
			clusterAckers = numWorkers / 2;
		}

		if (SystemParameters.getBoolean(map, "DIP_DISTRIBUTED")) {
			SystemParameters.putInMap(map, "DIP_NUM_ACKERS", clusterAckers);
			if (numWorkers + clusterAckers > SystemParameters.CLUSTER_SIZE)
				throw new RuntimeException("The cluster has only " + SystemParameters.CLUSTER_SIZE
						+ " nodes, but the query plan requires " + numWorkers + " workers "
						+ clusterAckers + " ackers.");
		} else
			SystemParameters.putInMap(map, "DIP_NUM_ACKERS", localAckers);

		return map;
	}

}