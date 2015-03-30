package ch.epfl.data.squall.query_plans.debug;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.EquiJoinComponent;
import ch.epfl.data.squall.components.OperatorComponent;
import ch.epfl.data.squall.conversion.IntegerConversion;
import ch.epfl.data.squall.operators.AggregateCountOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;

public class HyracksL3Plan extends QueryPlan {
	private static Logger LOG = Logger.getLogger(HyracksL3Plan.class);

	private final QueryBuilder _queryBuilder = new QueryBuilder();

	private static final IntegerConversion _ic = new IntegerConversion();

	public HyracksL3Plan(String dataPath, String extension, Map conf) {
		// -------------------------------------------------------------------------------------
		// start of query plan filling
		final ProjectOperator projectionCustomer = new ProjectOperator(
				new int[] { 0, 6 });
		final List<Integer> hashCustomer = Arrays.asList(0);
		final DataSourceComponent relationCustomer = new DataSourceComponent(
				"CUSTOMER", dataPath + "customer" + extension).add(
				projectionCustomer).setOutputPartKey(hashCustomer);
		_queryBuilder.add(relationCustomer);

		// -------------------------------------------------------------------------------------
		final ProjectOperator projectionOrders = new ProjectOperator(
				new int[] { 1 });
		final List<Integer> hashOrders = Arrays.asList(0);
		final DataSourceComponent relationOrders = new DataSourceComponent(
				"ORDERS", dataPath + "orders" + extension)
				.add(projectionOrders).setOutputPartKey(hashOrders);
		_queryBuilder.add(relationOrders);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashIndexes = Arrays.asList(1);
		final EquiJoinComponent CUSTOMER_ORDERSjoin = new EquiJoinComponent(
				relationCustomer, relationOrders).setOutputPartKey(hashIndexes);
		_queryBuilder.add(CUSTOMER_ORDERSjoin);

		// -------------------------------------------------------------------------------------
		final AggregateCountOperator agg = new AggregateCountOperator(conf)
				.setGroupByColumns(Arrays.asList(1));

		OperatorComponent oc = new OperatorComponent(CUSTOMER_ORDERSjoin,
				"COUNTAGG").add(agg).setFullHashList(
				Arrays.asList("FURNITURE", "BUILDING", "MACHINERY",
						"HOUSEHOLD", "AUTOMOBILE"));
		_queryBuilder.add(oc);

		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}

}
