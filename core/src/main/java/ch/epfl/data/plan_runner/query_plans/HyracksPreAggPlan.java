package ch.epfl.data.plan_runner.query_plans;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.components.EquiJoinComponent;
import ch.epfl.data.plan_runner.components.OperatorComponent;
import ch.epfl.data.plan_runner.conversion.DoubleConversion;
import ch.epfl.data.plan_runner.conversion.LongConversion;
import ch.epfl.data.plan_runner.conversion.StringConversion;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.expressions.ValueSpecification;
import ch.epfl.data.plan_runner.operators.AggregateCountOperator;
import ch.epfl.data.plan_runner.operators.AggregateSumOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.storage.AggregationStorage;

public class HyracksPreAggPlan extends QueryPlan {
	private static Logger LOG = Logger.getLogger(HyracksPreAggPlan.class);

	private final QueryBuilder _queryBuilder = new QueryBuilder();

	private static final DoubleConversion _dc = new DoubleConversion();
	private static final StringConversion _sc = new StringConversion();
	private static final LongConversion _lc = new LongConversion();

	public HyracksPreAggPlan(String dataPath, String extension, Map conf) {
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
		final ProjectOperator projFirstOut = new ProjectOperator(
				new ColumnReference(_sc, 1), new ValueSpecification(_sc, "1"));
		final ProjectOperator projSecondOut = new ProjectOperator(new int[] {
				1, 2 });
		final AggregationStorage secondJoinStorage = new AggregationStorage(
				new AggregateCountOperator(conf).setGroupByColumns(Arrays
						.asList(0)), _lc, conf, false);

		final List<Integer> hashIndexes = Arrays.asList(0);
		final EquiJoinComponent CUSTOMER_ORDERSjoin = new EquiJoinComponent(
				relationCustomer, relationOrders)
				.setFirstPreAggProj(projFirstOut)
				.setSecondPreAggProj(projSecondOut)
				.setSecondPreAggStorage(secondJoinStorage)
				.setOutputPartKey(hashIndexes);
		_queryBuilder.add(CUSTOMER_ORDERSjoin);

		// -------------------------------------------------------------------------------------
		final AggregateSumOperator agg = new AggregateSumOperator(
				new ColumnReference(_dc, 1), conf).setGroupByColumns(Arrays
				.asList(0));

		OperatorComponent oc = new OperatorComponent(CUSTOMER_ORDERSjoin,
				"COUNTAGG").add(agg);
		_queryBuilder.add(oc);

		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}

}
