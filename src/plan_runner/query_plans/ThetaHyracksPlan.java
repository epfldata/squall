package plan_runner.query_plans;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.DataSourceComponent;
import plan_runner.components.ThetaJoinDynamicComponentAdvisedEpochs;
import plan_runner.components.ThetaJoinStaticComponent;
import plan_runner.conversion.IntegerConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.operators.AggregateCountOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.predicates.ComparisonPredicate;

public class ThetaHyracksPlan {
	private static Logger LOG = Logger.getLogger(ThetaHyracksPlan.class);

	private final QueryPlan _queryPlan = new QueryPlan();

	private static final IntegerConversion _ic = new IntegerConversion();

	public ThetaHyracksPlan(String dataPath, String extension, Map conf) {
		final int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
		// -------------------------------------------------------------------------------------
		// start of query plan filling
		final ProjectOperator projectionCustomer = new ProjectOperator(new int[] { 0, 6 });
		final List<Integer> hashCustomer = Arrays.asList(0);
		final DataSourceComponent relationCustomer = new DataSourceComponent("CUSTOMER", dataPath
				+ "customer" + extension, _queryPlan).addOperator(projectionCustomer)
				.setHashIndexes(hashCustomer);

		// -------------------------------------------------------------------------------------
		final ProjectOperator projectionOrders = new ProjectOperator(new int[] { 1 });
		final List<Integer> hashOrders = Arrays.asList(0);
		final DataSourceComponent relationOrders = new DataSourceComponent("ORDERS", dataPath
				+ "orders" + extension, _queryPlan).addOperator(projectionOrders).setHashIndexes(
				hashOrders);

		// -------------------------------------------------------------------------------------

		final ColumnReference colCustomer = new ColumnReference(_ic, 0);
		final ColumnReference colOrders = new ColumnReference(_ic, 0);
		final ComparisonPredicate comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
				colCustomer, colOrders);

		final AggregateCountOperator agg = new AggregateCountOperator(conf)
				.setGroupByColumns(Arrays.asList(1));

		if (Theta_JoinType == 0)
			new ThetaJoinStaticComponent(relationCustomer, relationOrders, _queryPlan).addOperator(
					agg).setJoinPredicate(comp)
			// .addOperator(agg)
			;
		else if (Theta_JoinType == 3)
			new ThetaJoinDynamicComponentAdvisedEpochs(relationCustomer, relationOrders, _queryPlan)
					.addOperator(agg).setJoinPredicate(comp)
			// .addOperator(agg)
			;

		// -------------------------------------------------------------------------------------

	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}

}