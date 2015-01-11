package ch.epfl.data.plan_runner.query_plans.theta;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.plan_runner.components.theta.ThetaJoinDynamicComponentAdvisedEpochs;
import ch.epfl.data.plan_runner.components.theta.ThetaJoinStaticComponent;
import ch.epfl.data.plan_runner.conversion.DoubleConversion;
import ch.epfl.data.plan_runner.conversion.NumericConversion;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.AggregateSumOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;

public class ThetaOutputDominatedPlan {

	private static Logger LOG = Logger.getLogger(ThetaOutputDominatedPlan.class);

	private final QueryBuilder _queryBuilder = new QueryBuilder();

	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();

	/*
	 * SELECT SUM(SUPPLIER.SUPPKEY) FROM SUPPLIER, NATION
	 */
	public ThetaOutputDominatedPlan(String dataPath, String extension, Map conf) {
		final int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
		// -------------------------------------------------------------------------------------
		final List<Integer> hashSupplier = Arrays.asList(0);

		final ProjectOperator projectionSupplier = new ProjectOperator(new int[] { 0 });

		final DataSourceComponent relationSupplier = new DataSourceComponent("SUPPLIER", dataPath
				+ "supplier" + extension).add(projectionSupplier)
				.setOutputPartKey(hashSupplier);
		_queryBuilder.add(relationSupplier);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashNation = Arrays.asList(0);

		final ProjectOperator projectionNation = new ProjectOperator(new int[] { 1 });

		final DataSourceComponent relationNation = new DataSourceComponent("NATION", dataPath
				+ "nation" + extension).add(projectionNation).setOutputPartKey(
				hashNation);
		_queryBuilder.add(relationNation);

		final AggregateOperator agg = new AggregateSumOperator(new ColumnReference(_doubleConv, 0),
				conf);
		// Empty parameters = Cartesian Product

		Component lastJoiner = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationSupplier, relationNation,
						_queryBuilder).add(new ProjectOperator(new int[] { 0 }))
				.add(agg);
		//lastJoiner.setPrintOut(false);

		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}