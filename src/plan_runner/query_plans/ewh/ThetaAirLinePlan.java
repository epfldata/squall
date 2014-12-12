package plan_runner.query_plans.ewh;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.theta.ThetaJoinComponentFactory;
import plan_runner.components.theta.ThetaJoinStaticComponent;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateCountOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.query_plans.QueryPlan;
import plan_runner.query_plans.theta.ThetaQueryPlansParameters;

public class ThetaAirLinePlan {
	private QueryPlan _queryPlan = new QueryPlan();
	private static final IntegerConversion _ic = new IntegerConversion();
	private static final TypeConversion<String> _stringConv = new StringConversion();

	public ThetaAirLinePlan(String dataPath, String extension, Map conf) {

		int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);

		ProjectOperator projectionLineitem = new ProjectOperator(new int[] { 18 });
		final List<Integer> hashAir = Arrays.asList(0);
		
		ComparisonPredicate comp = new ComparisonPredicate(ComparisonPredicate.NONEQUAL_OP,
				new ColumnReference(_stringConv, 0), new ValueSpecification(_stringConv, "NA"));
		SelectOperator selection = new SelectOperator(comp);

		DataSourceComponent relationAirLine1 = new DataSourceComponent("AIR1", dataPath
				+ "2004to2007" + extension, _queryPlan).addOperator(projectionLineitem)
				.addOperator(selection).setHashIndexes(hashAir);

		DataSourceComponent relationAirLine2 = new DataSourceComponent("AIR2", dataPath + "1987"
				+ extension, _queryPlan).addOperator(projectionLineitem).addOperator(selection).setHashIndexes(hashAir);

		ColumnReference colAir1 = new ColumnReference(_ic, 0); //Distance
		ColumnReference colAir2 = new ColumnReference(_ic, 0); //Distance

		//		Addition add = new Addition(colAir2, new ValueSpecification(_ic,2));
		ComparisonPredicate pred = new ComparisonPredicate(ComparisonPredicate.NONGREATER_OP,
				colAir1, colAir2, 2, ComparisonPredicate.BPLUSTREE);

		AggregateCountOperator agg = new AggregateCountOperator(conf);

		Component AIR1_AIR2 = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationAirLine1, relationAirLine2,
						_queryPlan).setJoinPredicate(pred).addOperator(agg).setContentSensitiveThetaJoinWrapper(_ic);

	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}
