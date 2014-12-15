package plan_runner.query_plans;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.Multiplication;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.ComparisonPredicate;

public class RSTPlan {
	private static Logger LOG = Logger.getLogger(RSTPlan.class);

	private static final NumericConversion<Double> _dc = new DoubleConversion();
	private static final NumericConversion<Integer> _ic = new IntegerConversion();

	private final QueryBuilder _queryPlan = new QueryBuilder();

	public RSTPlan(String dataPath, String extension, Map conf) {
		// -------------------------------------------------------------------------------------
		// start of query plan filling
		final List<Integer> hashR = Arrays.asList(1);

		final DataSourceComponent relationR = new DataSourceComponent("R", dataPath + "r"
				+ extension, _queryPlan).setHashIndexes(hashR);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashS = Arrays.asList(0);

		final DataSourceComponent relationS = new DataSourceComponent("S", dataPath + "s"
				+ extension, _queryPlan).setHashIndexes(hashS);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashIndexes = Arrays.asList(2);

		final EquiJoinComponent R_Sjoin = new EquiJoinComponent(relationR, relationS, _queryPlan)
				.setHashIndexes(hashIndexes);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashT = Arrays.asList(0);

		final DataSourceComponent relationT = new DataSourceComponent("T", dataPath + "t"
				+ extension, _queryPlan).setHashIndexes(hashT);

		// -------------------------------------------------------------------------------------
		final ValueExpression<Double> aggVe = new Multiplication(new ColumnReference(_dc, 0),
				new ColumnReference(_dc, 3));

		final AggregateSumOperator sp = new AggregateSumOperator(aggVe, conf);

		new EquiJoinComponent(R_Sjoin, relationT, _queryPlan).addOperator(
				new SelectOperator(new ComparisonPredicate(new ColumnReference(_ic, 1),
						new ValueSpecification(_ic, 10)))).addOperator(sp);

	}

	public QueryBuilder getQueryPlan() {
		return _queryPlan;
	}
}
