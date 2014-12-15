package plan_runner.query_plans;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.conversion.IntegerConversion;
import plan_runner.operators.AggregateCountOperator;
import plan_runner.operators.ProjectOperator;

public class HyracksPlan {
	private static Logger LOG = Logger.getLogger(HyracksPlan.class);

	private final QueryBuilder _queryBuilder = new QueryBuilder();

	private static final IntegerConversion _ic = new IntegerConversion();

	public HyracksPlan(String dataPath, String extension, Map conf) {
		// -------------------------------------------------------------------------------------
		// start of query plan filling
		final ProjectOperator projectionCustomer = new ProjectOperator(new int[] { 0, 6 });
		final List<Integer> hashCustomer = Arrays.asList(0);
		final DataSourceComponent relationCustomer = new DataSourceComponent("CUSTOMER", dataPath
				+ "customer" + extension).addOperator(projectionCustomer)
				.setHashIndexes(hashCustomer);
		_queryBuilder.add(relationCustomer);

		// -------------------------------------------------------------------------------------
		final ProjectOperator projectionOrders = new ProjectOperator(new int[] { 1 });
		final List<Integer> hashOrders = Arrays.asList(0);
		final DataSourceComponent relationOrders = new DataSourceComponent("ORDERS", dataPath
				+ "orders" + extension).addOperator(projectionOrders).setHashIndexes(
				hashOrders);
		_queryBuilder.add(relationOrders);

		// -------------------------------------------------------------------------------------

		final AggregateCountOperator agg = new AggregateCountOperator(conf)
				.setGroupByColumns(Arrays.asList(1));

		final EquiJoinComponent joinCustOrders = new EquiJoinComponent(relationCustomer, relationOrders).addOperator(agg);
		_queryBuilder.add(joinCustOrders);

		// REMOVE THIS
		// settBolt(String.valueOf(_ID), this, _reshufflerParallelism);
		
		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryBuilder() {
		return _queryBuilder;
	}

}