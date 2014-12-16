package plan_runner.query_plans.debug;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.DataSourceComponent;
import plan_runner.conversion.IntegerConversion;
import plan_runner.operators.ProjectOperator;
import plan_runner.query_plans.QueryBuilder;

public class HyracksL1Plan {
	private static Logger LOG = Logger.getLogger(HyracksL1Plan.class);

	private final QueryBuilder _queryBuilder = new QueryBuilder();

	private static final IntegerConversion _ic = new IntegerConversion();

	public HyracksL1Plan(String dataPath, String extension, Map conf) {
		// -------------------------------------------------------------------------------------
		// start of query plan filling
		final ProjectOperator projectionCustomer = new ProjectOperator(new int[] { 0, 6 });
		final List<Integer> hashCustomer = Arrays.asList(0);
		DataSourceComponent relationCustomer = new DataSourceComponent("CUSTOMER", dataPath + "customer" + extension)
				.add(projectionCustomer).setOutputPartKey(hashCustomer).setPrintOut(false);
		_queryBuilder.add(relationCustomer);

		// -------------------------------------------------------------------------------------
		final ProjectOperator projectionOrders = new ProjectOperator(new int[] { 1 });
		final List<Integer> hashOrders = Arrays.asList(0);
		DataSourceComponent relationOrders = new DataSourceComponent("ORDERS", dataPath + "orders" + extension)
				.add(projectionOrders).setOutputPartKey(hashOrders).setPrintOut(false);
		_queryBuilder.add(relationOrders);

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}

}