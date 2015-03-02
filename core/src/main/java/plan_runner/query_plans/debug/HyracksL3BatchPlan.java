package plan_runner.query_plans.debug;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.components.OperatorComponent;
import plan_runner.conversion.IntegerConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.operators.AggregateCountOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.query_plans.QueryBuilder;

public class HyracksL3BatchPlan {
	private static Logger LOG = Logger.getLogger(HyracksL3BatchPlan.class);

	private final QueryBuilder _queryBuilder = new QueryBuilder();

	private static final IntegerConversion _ic = new IntegerConversion();

	public HyracksL3BatchPlan(String dataPath, String extension, Map conf) {
		// -------------------------------------------------------------------------------------
		// start of query plan filling
		final ProjectOperator projectionCustomer = new ProjectOperator(new int[] { 0, 6 });
		final List<Integer> hashCustomer = Arrays.asList(0);
		final DataSourceComponent relationCustomer = new DataSourceComponent("CUSTOMER", dataPath
				+ "customer" + extension).add(projectionCustomer)
				.setOutputPartKey(hashCustomer);
		_queryBuilder.add(relationCustomer);

		// -------------------------------------------------------------------------------------
		final ProjectOperator projectionOrders = new ProjectOperator(new int[] { 1 });
		final List<Integer> hashOrders = Arrays.asList(0);
		final DataSourceComponent relationOrders = new DataSourceComponent("ORDERS", dataPath
				+ "orders" + extension).add(projectionOrders).setOutputPartKey(
				hashOrders);
		_queryBuilder.add(relationOrders);

		// -------------------------------------------------------------------------------------

		final AggregateCountOperator postAgg = new AggregateCountOperator(conf)
				.setGroupByColumns(Arrays.asList(1));
		final List<Integer> hashIndexes = Arrays.asList(0);
		final EquiJoinComponent CUSTOMER_ORDERSjoin = new EquiJoinComponent(relationCustomer,
				relationOrders).add(postAgg).setOutputPartKey(hashIndexes)
				.setBatchOutputMillis(1000);
		_queryBuilder.add(CUSTOMER_ORDERSjoin);

		// -------------------------------------------------------------------------------------
		final AggregateSumOperator agg = new AggregateSumOperator(new ColumnReference(_ic, 1), conf)
				.setGroupByColumns(Arrays.asList(0));

		OperatorComponent oc = new OperatorComponent(CUSTOMER_ORDERSjoin, "COUNTAGG").add(agg)
				.setFullHashList(
						Arrays.asList("FURNITURE", "BUILDING", "MACHINERY", "HOUSEHOLD",
								"AUTOMOBILE"));
		_queryBuilder.add(oc);

		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}

}