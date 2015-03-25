package ch.epfl.data.plan_runner.query_plans;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.components.EquiJoinComponent;
import ch.epfl.data.plan_runner.conversion.DateConversion;
import ch.epfl.data.plan_runner.conversion.DoubleConversion;
import ch.epfl.data.plan_runner.conversion.NumericConversion;
import ch.epfl.data.plan_runner.conversion.StringConversion;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.expressions.DateSum;
import ch.epfl.data.plan_runner.expressions.Multiplication;
import ch.epfl.data.plan_runner.expressions.Subtraction;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.expressions.ValueSpecification;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.AggregateSumOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.operators.SelectOperator;
import ch.epfl.data.plan_runner.predicates.BetweenPredicate;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;

public class TPCH10Plan extends QueryPlan {
	private static void computeDates() {
		// date2= date1 + 3 months
		final String date1Str = "1993-10-01";
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

	private static Logger LOG = Logger.getLogger(TPCH10Plan.class);
	private static final TypeConversion<Date> _dc = new DateConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final StringConversion _sc = new StringConversion();

	private final QueryBuilder _queryBuilder = new QueryBuilder();

	// query variables
	private static Date _date1, _date2;

	public TPCH10Plan(String dataPath, String extension, Map conf) {
		computeDates();

		// -------------------------------------------------------------------------------------
		final List<Integer> hashCustomer = Arrays.asList(0);

		final ProjectOperator projectionCustomer = new ProjectOperator(
				new int[] { 0, 1, 2, 3, 4, 5, 7 });

		final DataSourceComponent relationCustomer = new DataSourceComponent(
				"CUSTOMER", dataPath + "customer" + extension)
				.setOutputPartKey(hashCustomer).add(projectionCustomer);
		_queryBuilder.add(relationCustomer);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashOrders = Arrays.asList(1);

		final SelectOperator selectionOrders = new SelectOperator(
				new BetweenPredicate(new ColumnReference(_dc, 4), true,
						new ValueSpecification(_dc, _date1), false,
						new ValueSpecification(_dc, _date2)));

		final ProjectOperator projectionOrders = new ProjectOperator(new int[] {
				0, 1 });

		final DataSourceComponent relationOrders = new DataSourceComponent(
				"ORDERS", dataPath + "orders" + extension)
				.setOutputPartKey(hashOrders).add(selectionOrders)
				.add(projectionOrders);
		_queryBuilder.add(relationOrders);

		// -------------------------------------------------------------------------------------
		final EquiJoinComponent C_Ojoin = new EquiJoinComponent(
				relationCustomer, relationOrders).setOutputPartKey(Arrays
				.asList(3));
		_queryBuilder.add(C_Ojoin);
		// -------------------------------------------------------------------------------------
		final List<Integer> hashNation = Arrays.asList(0);

		final ProjectOperator projectionNation = new ProjectOperator(new int[] {
				0, 1 });

		final DataSourceComponent relationNation = new DataSourceComponent(
				"NATION", dataPath + "nation" + extension).setOutputPartKey(
				hashNation).add(projectionNation);
		_queryBuilder.add(relationNation);
		// -------------------------------------------------------------------------------------

		final EquiJoinComponent C_O_Njoin = new EquiJoinComponent(C_Ojoin,
				relationNation).add(
				new ProjectOperator(new int[] { 0, 1, 2, 4, 5, 6, 7, 8 }))
				.setOutputPartKey(Arrays.asList(6));
		_queryBuilder.add(C_O_Njoin);
		// -------------------------------------------------------------------------------------

		final List<Integer> hashLineitem = Arrays.asList(0);

		final SelectOperator selectionLineitem = new SelectOperator(
				new ComparisonPredicate(new ColumnReference(_sc, 8),
						new ValueSpecification(_sc, "R")));

		final ProjectOperator projectionLineitem = new ProjectOperator(
				new int[] { 0, 5, 6 });

		final DataSourceComponent relationLineitem = new DataSourceComponent(
				"LINEITEM", dataPath + "lineitem" + extension)
				.setOutputPartKey(hashLineitem).add(selectionLineitem)
				.add(projectionLineitem);
		_queryBuilder.add(relationLineitem);

		// -------------------------------------------------------------------------------------
		// set up aggregation function on the StormComponent(Bolt) where join is
		// performed

		// 1 - discount
		final ValueExpression<Double> substract = new Subtraction(
				new ValueSpecification(_doubleConv, 1.0), new ColumnReference(
						_doubleConv, 8));
		// extendedPrice*(1-discount)
		final ValueExpression<Double> product = new Multiplication(
				new ColumnReference(_doubleConv, 7), substract);
		final AggregateOperator agg = new AggregateSumOperator(product, conf)
				.setGroupByColumns(Arrays.asList(0, 1, 4, 6, 2, 3, 5));

		EquiJoinComponent finalComp = new EquiJoinComponent(C_O_Njoin,
				relationLineitem).add(
				new ProjectOperator(new int[] { 0, 1, 2, 3, 4, 5, 7, 8, 9 }))
				.add(agg);
		_queryBuilder.add(finalComp);

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
