package plan_runner.query_plans;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.components.OperatorComponent;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.LongConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateCountOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.storage.AggregationStorage;

public class HyracksPreAggPlan {
	private static Logger LOG = Logger.getLogger(HyracksPreAggPlan.class);

	private final QueryBuilder _queryBuilder = new QueryBuilder();

	private static final DoubleConversion _dc = new DoubleConversion();
	private static final StringConversion _sc = new StringConversion();
	private static final LongConversion _lc = new LongConversion();

	public HyracksPreAggPlan(String dataPath, String extension, Map conf) {
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
		final ProjectOperator projFirstOut = new ProjectOperator(new ColumnReference(_sc, 1),
				new ValueSpecification(_sc, "1"));
		final ProjectOperator projSecondOut = new ProjectOperator(new int[] { 1, 2 });
		final AggregationStorage secondJoinStorage = new AggregationStorage(
				new AggregateCountOperator(conf).setGroupByColumns(Arrays.asList(0)), _lc, conf,
				false);

		final List<Integer> hashIndexes = Arrays.asList(0);
		final EquiJoinComponent CUSTOMER_ORDERSjoin = new EquiJoinComponent(relationCustomer,
				relationOrders).setFirstPreAggProj(projFirstOut)
				.setSecondPreAggProj(projSecondOut).setSecondPreAggStorage(secondJoinStorage)
				.setHashIndexes(hashIndexes);
		_queryBuilder.add(CUSTOMER_ORDERSjoin);

		// -------------------------------------------------------------------------------------
		final AggregateSumOperator agg = new AggregateSumOperator(new ColumnReference(_dc, 1), conf)
				.setGroupByColumns(Arrays.asList(0));

		OperatorComponent oc = 
				new OperatorComponent(CUSTOMER_ORDERSjoin, "COUNTAGG")
				.addOperator(agg);
		_queryBuilder.add(oc);

		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}

}
