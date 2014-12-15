package plan_runner.query_plans;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.components.OperatorComponent;
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

/* TPC_H Query 4 - Order Priority Checking
 SELECT O_ORDERPRIORITY, COUNT(*) AS ORDER_COUNT FROM ORDERS
 WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < dateadd(mm,3, cast('1993-07-01' as date))
 AND EXISTS (SELECT * FROM LINEITEM WHERE L_ORDERKEY = O_ORDERKEY AND L_COMMITDATE < L_RECEIPTDATE)
 GROUP BY O_ORDERPRIORITY
 ORDER BY O_ORDERPRIORITY
 */

public class TPCH4Plan {
	private static Logger LOG = Logger.getLogger(TPCH4Plan.class);

	private static final TypeConversion<Date> _dc = new DateConversion();
	private static final IntegerConversion _ic = new IntegerConversion();
	private final QueryBuilder _queryPlan = new QueryBuilder();

	// query variables
	private static Date _date1, _date2;

	private static void computeDates() {
		// date2= date1 + 3 months
		final String date1Str = "1993-07-01";
		final int interval = 3;
		final int unit = Calendar.MONTH;

		// setting _date1
		_date1 = _dc.fromString(date1Str);

		// setting _date2
		ValueExpression<Date> date1Ve, date2Ve;
		date1Ve = new ValueSpecification<Date>(_dc, _date1);
		date2Ve = new DateSum(date1Ve, unit, interval);
		_date2 = date2Ve.eval(null);
		// tuple is set to null since we are computing based on constants
	}

	public TPCH4Plan(String dataPath, String extension, Map conf) {
		computeDates();

		// -------------------------------------------------------------------------------------
		final List<Integer> hashLineitem = Arrays.asList(0);

		final SelectOperator selectionLineitem = new SelectOperator(new ComparisonPredicate(
				ComparisonPredicate.LESS_OP, new ColumnReference(_dc, 11), new ColumnReference(_dc,
						12)));

		final ProjectOperator projectionLineitem = new ProjectOperator(new int[] { 0 });

		final DataSourceComponent relationLineitem = new DataSourceComponent("LINEITEM", dataPath
				+ "lineitem" + extension, _queryPlan).setHashIndexes(hashLineitem)
				.addOperator(selectionLineitem).addOperator(projectionLineitem);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashOrders = Arrays.asList(0);

		final SelectOperator selectionOrders = new SelectOperator(new BetweenPredicate(
				new ColumnReference(_dc, 4), true, new ValueSpecification(_dc, _date1), false,
				new ValueSpecification(_dc, _date2)));

		final ProjectOperator projectionOrders = new ProjectOperator(new int[] { 0, 5 });

		final DataSourceComponent relationOrders = new DataSourceComponent("ORDERS", dataPath
				+ "orders" + extension, _queryPlan).setHashIndexes(hashOrders)
				.addOperator(selectionOrders).addOperator(projectionOrders);

		// -------------------------------------------------------------------------------------
		final EquiJoinComponent O_Ljoin = new EquiJoinComponent(relationOrders, relationLineitem,
				_queryPlan).setHashIndexes(Arrays.asList(1));

		// -------------------------------------------------------------------------------------
		// set up aggregation function on a separate StormComponent(Bolt)
		final DistinctOperator distinctOp = new DistinctOperator(conf, new int[] { 0 });
		final AggregateOperator aggOp = new AggregateCountOperator(conf).setGroupByColumns(
				Arrays.asList(1)).setDistinct(distinctOp);
		new OperatorComponent(O_Ljoin, "FINAL_RESULT", _queryPlan).addOperator(aggOp);

		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryPlan;
	}
}
