package ch.epfl.data.squall.query_plans.theta;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.OperatorComponent;
import ch.epfl.data.squall.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.squall.conversion.DateConversion;
import ch.epfl.data.squall.conversion.IntegerConversion;
import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.DateSum;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.AggregateCountOperator;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.DistinctOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.BetweenPredicate;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;

public class ThetaTPCH4Plan extends QueryPlan {
	private static void computeDates() {
		// date2= date1 + 3 months
		String date1Str = "1993-07-01";
		int interval = 3;
		int unit = Calendar.MONTH;

		// setting _date1
		_date1 = _dc.fromString(date1Str);

		// setting _date2
		ValueExpression<Date> date1Ve, date2Ve;
		date1Ve = new ValueSpecification<Date>(_dc, _date1);
		date2Ve = new DateSum(date1Ve, unit, interval);
		_date2 = date2Ve.eval(null);
		// tuple is set to null since we are computing based on constants
	}

	private static Logger LOG = Logger.getLogger(ThetaTPCH4Plan.class);
	private static final TypeConversion<Date> _dc = new DateConversion();
	private static final IntegerConversion _ic = new IntegerConversion();

	private QueryBuilder _queryBuilder = new QueryBuilder();

	// query variables
	private static Date _date1, _date2;

	public ThetaTPCH4Plan(String dataPath, String extension, Map conf) {
		computeDates();

		int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
		// -------------------------------------------------------------------------------------
		List<Integer> hashOrders = Arrays.asList(0);

		SelectOperator selectionOrders = new SelectOperator(
				new BetweenPredicate(new ColumnReference(_dc, 4), true,
						new ValueSpecification(_dc, _date1), false,
						new ValueSpecification(_dc, _date2)));

		ProjectOperator projectionOrders = new ProjectOperator(
				new int[] { 0, 5 });

		DataSourceComponent relationOrders = new DataSourceComponent("ORDERS",
				dataPath + "orders" + extension).setOutputPartKey(hashOrders)
				.add(selectionOrders).add(projectionOrders);
		_queryBuilder.add(relationOrders);

		// -------------------------------------------------------------------------------------
		List<Integer> hashLineitem = Arrays.asList(0);

		SelectOperator selectionLineitem = new SelectOperator(
				new ComparisonPredicate(ComparisonPredicate.LESS_OP,
						new ColumnReference(_dc, 11), new ColumnReference(_dc,
								12)));

		DistinctOperator distinctLineitem = new DistinctOperator(conf,
				new int[] { 0 });

		DataSourceComponent relationLineitem = new DataSourceComponent(
				"LINEITEM", dataPath + "lineitem" + extension)
				.setOutputPartKey(hashLineitem).add(selectionLineitem)
				.add(distinctLineitem);
		_queryBuilder.add(relationLineitem);

		// -------------------------------------------------------------------------------------

		ColumnReference colO = new ColumnReference(_ic, 0);
		ColumnReference colL = new ColumnReference(_ic, 0);
		ComparisonPredicate O_L_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colO, colL);

		Component O_Ljoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationOrders,
						relationLineitem, _queryBuilder)
				.setOutputPartKey(Arrays.asList(1)).setJoinPredicate(O_L_comp);

		// -------------------------------------------------------------------------------------
		// set up aggregation function on a separate StormComponent(Bolt)
		AggregateOperator aggOp = new AggregateCountOperator(conf)
				.setGroupByColumns(Arrays.asList(1));
		OperatorComponent finalComponent = new OperatorComponent(O_Ljoin,
				"FINAL_RESULT").add(aggOp);
		_queryBuilder.add(finalComponent);

		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
