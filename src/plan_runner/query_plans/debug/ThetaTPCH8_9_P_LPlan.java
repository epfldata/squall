package plan_runner.query_plans.debug;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.theta.ThetaJoinComponentFactory;
import plan_runner.components.theta.ThetaJoinDynamicComponentAdvisedEpochs;
import plan_runner.components.theta.ThetaJoinStaticComponent;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.operators.ProjectOperator;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.query_plans.QueryBuilder;
import plan_runner.query_plans.theta.ThetaQueryPlansParameters;

public class ThetaTPCH8_9_P_LPlan {
	private static Logger LOG = Logger.getLogger(ThetaTPCH8_9_P_LPlan.class);

	private static final StringConversion _sc = new StringConversion();
	private static final IntegerConversion _ic = new IntegerConversion();

	private QueryBuilder _queryBuilder = new QueryBuilder();

	public ThetaTPCH8_9_P_LPlan(String dataPath, String extension, Map conf) {
		int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
		//-------------------------------------------------------------------------------------
		List<Integer> hashPart = Arrays.asList(0);

		ProjectOperator projectionPart = new ProjectOperator(new int[] { 0 });

		DataSourceComponent relationPart = new DataSourceComponent("PART", dataPath + "part"
				+ extension).setOutputPartKey(hashPart).add(projectionPart);
		_queryBuilder.add(relationPart);

		//-------------------------------------------------------------------------------------
		List<Integer> hashLineitem = Arrays.asList(1);

		ProjectOperator projectionLineitem = new ProjectOperator(new int[] { 0, 1, 2, 4, 5, 6 });

		DataSourceComponent relationLineitem = new DataSourceComponent("LINEITEM", dataPath
				+ "lineitem" + extension).setOutputPartKey(hashLineitem).add(
				projectionLineitem);
		_queryBuilder.add(relationLineitem);

		//-------------------------------------------------------------------------------------
		//TODO
		ColumnReference colP = new ColumnReference(_ic, 0);
		ColumnReference colL = new ColumnReference(_ic, 1);
		ComparisonPredicate P_L_comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, colP,
				colL);

		//        AggregateCountOperator agg= new AggregateCountOperator(conf);

		Component P_Ljoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationPart, relationLineitem, _queryBuilder)
				.setOutputPartKey(Arrays.asList(0, 2)).setJoinPredicate(P_L_comp)
				.add(new ProjectOperator(new int[] { 0, 1, 3, 4, 5, 6 })).setContentSensitiveThetaJoinWrapper(_ic)
		//					.addOperator(agg)
		;

		P_Ljoin.setPrintOut(false);

		//-------------------------------------------------------------------------------------
	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
