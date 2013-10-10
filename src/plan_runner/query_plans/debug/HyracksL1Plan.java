package plan_runner.query_plans.debug;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.DataSourceComponent;
import plan_runner.conversion.IntegerConversion;
import plan_runner.operators.ProjectOperator;
import plan_runner.query_plans.QueryPlan;

public class HyracksL1Plan {
	private static Logger LOG = Logger.getLogger(HyracksL1Plan.class);

	private final QueryPlan _queryPlan = new QueryPlan();

	private static final IntegerConversion _ic = new IntegerConversion();

	public HyracksL1Plan(String dataPath, String extension, Map conf) {
		// -------------------------------------------------------------------------------------
		// start of query plan filling
		final ProjectOperator projectionCustomer = new ProjectOperator(new int[] { 0, 6 });
		final List<Integer> hashCustomer = Arrays.asList(0);
		new DataSourceComponent("CUSTOMER", dataPath + "customer" + extension, _queryPlan)
				.addOperator(projectionCustomer).setHashIndexes(hashCustomer).setPrintOut(false);

		// -------------------------------------------------------------------------------------
		final ProjectOperator projectionOrders = new ProjectOperator(new int[] { 1 });
		final List<Integer> hashOrders = Arrays.asList(0);
		new DataSourceComponent("ORDERS", dataPath + "orders" + extension, _queryPlan)
				.addOperator(projectionOrders).setHashIndexes(hashOrders).setPrintOut(false);

	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}

}