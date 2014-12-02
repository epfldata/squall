package plan_runner.query_plans;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.OperatorComponent;
import plan_runner.components.ThetaJoinComponentFactory;
import plan_runner.components.ThetaJoinDynamicComponentAdvisedEpochs;
import plan_runner.components.ThetaJoinStaticComponent;
import plan_runner.conversion.DateConversion;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.DateSum;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateCountOperator;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.DistinctOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.BetweenPredicate;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.query_plans.QueryPlan;
import plan_runner.query_plans.ThetaQueryPlansParameters;

public class ThetaTPCH4Plan {
	private static Logger LOG = Logger.getLogger(ThetaTPCH4Plan.class);

	private static final TypeConversion<Date> _dc = new DateConversion();
	private static final IntegerConversion _ic = new IntegerConversion();
	private QueryPlan _queryPlan = new QueryPlan();

	//query variables
	private static Date _date1, _date2;

	private static void computeDates() {
		// date2= date1 + 3 months
		String date1Str = "1993-07-01";
		int interval = 3;
		int unit = Calendar.MONTH;

		//setting _date1
		_date1 = _dc.fromString(date1Str);

		//setting _date2
		ValueExpression<Date> date1Ve, date2Ve;
		date1Ve = new ValueSpecification<Date>(_dc, _date1);
		date2Ve = new DateSum(date1Ve, unit, interval);
		_date2 = date2Ve.eval(null);
		// tuple is set to null since we are computing based on constants
	}

	public ThetaTPCH4Plan(String dataPath, String extension, Map conf) {
		computeDates();

		int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
		//-------------------------------------------------------------------------------------
		List<Integer> hashOrders = Arrays.asList(0);

		SelectOperator selectionOrders = new SelectOperator(new BetweenPredicate(
				new ColumnReference(_dc, 4), true, new ValueSpecification(_dc, _date1), false,
				new ValueSpecification(_dc, _date2)));

		ProjectOperator projectionOrders = new ProjectOperator(new int[] { 0, 5 });

		DataSourceComponent relationOrders = new DataSourceComponent("ORDERS", dataPath + "orders"
				+ extension, _queryPlan).setHashIndexes(hashOrders).addOperator(selectionOrders)
				.addOperator(projectionOrders);

		//-------------------------------------------------------------------------------------
		List<Integer> hashLineitem = Arrays.asList(0);

		SelectOperator selectionLineitem = new SelectOperator(new ComparisonPredicate(
				ComparisonPredicate.LESS_OP, new ColumnReference(_dc, 11), new ColumnReference(_dc,
						12)));

		DistinctOperator distinctLineitem = new DistinctOperator(conf, new int[] { 0 });

		DataSourceComponent relationLineitem = new DataSourceComponent("LINEITEM", dataPath
				+ "lineitem" + extension, _queryPlan).setHashIndexes(hashLineitem)
				.addOperator(selectionLineitem).addOperator(distinctLineitem);

		//-------------------------------------------------------------------------------------

		ColumnReference colO = new ColumnReference(_ic, 0);
		ColumnReference colL = new ColumnReference(_ic, 0);
		ComparisonPredicate O_L_comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, colO,
				colL);

		Component O_Ljoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationOrders, relationLineitem,
						_queryPlan).setHashIndexes(Arrays.asList(1)).setJoinPredicate(O_L_comp);

		//-------------------------------------------------------------------------------------
		// set up aggregation function on a separate StormComponent(Bolt)
		AggregateOperator aggOp = new AggregateCountOperator(conf).setGroupByColumns(Arrays
				.asList(1));
		OperatorComponent finalComponent = new OperatorComponent(O_Ljoin, "FINAL_RESULT",
				_queryPlan).addOperator(aggOp);

		//-------------------------------------------------------------------------------------

	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}
