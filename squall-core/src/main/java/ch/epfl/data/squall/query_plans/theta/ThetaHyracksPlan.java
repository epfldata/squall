package ch.epfl.data.squall.query_plans.theta;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.squall.conversion.IntegerConversion;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.operators.AggregateCountOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;

public class ThetaHyracksPlan extends QueryPlan {
	private static Logger LOG = Logger.getLogger(ThetaHyracksPlan.class);

	private final QueryBuilder _queryBuilder = new QueryBuilder();

	private static final IntegerConversion _ic = new IntegerConversion();

	public ThetaHyracksPlan(String dataPath, String extension, Map conf) {
		final int Theta_JoinType = ThetaQueryPlansParameters
				.getThetaJoinType(conf);
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

		final ColumnReference colCustomer = new ColumnReference(_ic, 0);
		final ColumnReference colOrders = new ColumnReference(_ic, 0);
		final ComparisonPredicate comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colCustomer, colOrders);

		final AggregateCountOperator agg = new AggregateCountOperator(conf)
				.setGroupByColumns(Arrays.asList(1));

		Component lastJoiner = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationCustomer,
						relationOrders, _queryBuilder).add(agg)
				.setJoinPredicate(comp)
				.setContentSensitiveThetaJoinWrapper(_ic);

		// lastJoiner.setPrintOut(false);

		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}

}
