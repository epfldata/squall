package ch.epfl.data.plan_runner.query_plans.debug;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.components.EquiJoinComponent;
import ch.epfl.data.plan_runner.components.OperatorComponent;
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
import ch.epfl.data.plan_runner.operators.AggregateAvgOperator;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.operators.SelectOperator;
import ch.epfl.data.plan_runner.predicates.BetweenPredicate;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;

public class TPCH5PlanAvg {
	private static void computeDates() {
		// date2 = date1 + 1 year
		final String date1Str = "1994-01-01";
		final int interval = 1;
		final int unit = Calendar.YEAR;

		// setting _date1
		_date1 = _dc.fromString(date1Str);

		// setting _date2
		ValueExpression<Date> date1Ve, date2Ve;
		date1Ve = new ValueSpecification<Date>(_dc, _date1);
		date2Ve = new DateSum(date1Ve, unit, interval);
		_date2 = date2Ve.eval(null);
		// tuple is set to null since we are computing based on constants
	}

	private static Logger LOG = Logger.getLogger(TPCH5PlanAvg.class);
	private static final TypeConversion<Date> _dc = new DateConversion();
	private static final TypeConversion<String> _sc = new StringConversion();

	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();

	private final QueryBuilder _queryBuilder = new QueryBuilder();
	// query variables
	private static Date _date1, _date2;

	private static final String REGION_NAME = "ASIA";

	public TPCH5PlanAvg(String dataPath, String extension, Map conf) {
		computeDates();

		// -------------------------------------------------------------------------------------
		final List<Integer> hashRegion = Arrays.asList(0);

		final SelectOperator selectionRegion = new SelectOperator(
				new ComparisonPredicate(new ColumnReference(_sc, 1),
						new ValueSpecification(_sc, REGION_NAME)));

		final ProjectOperator projectionRegion = new ProjectOperator(
				new int[] { 0 });

		final DataSourceComponent relationRegion = new DataSourceComponent(
				"REGION", dataPath + "region" + extension)
				.setOutputPartKey(hashRegion).add(selectionRegion)
				.add(projectionRegion);
		_queryBuilder.add(relationRegion);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashNation = Arrays.asList(2);

		final ProjectOperator projectionNation = new ProjectOperator(new int[] {
				0, 1, 2 });

		final DataSourceComponent relationNation = new DataSourceComponent(
				"NATION", dataPath + "nation" + extension).setOutputPartKey(
				hashNation).add(projectionNation);
		_queryBuilder.add(relationNation);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashRN = Arrays.asList(0);

		final ProjectOperator projectionRN = new ProjectOperator(new int[] { 1,
				2 });

		final EquiJoinComponent R_Njoin = new EquiJoinComponent(relationRegion,
				relationNation).setOutputPartKey(hashRN).add(projectionRN);
		_queryBuilder.add(R_Njoin);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashSupplier = Arrays.asList(1);

		final ProjectOperator projectionSupplier = new ProjectOperator(
				new int[] { 0, 3 });

		final DataSourceComponent relationSupplier = new DataSourceComponent(
				"SUPPLIER", dataPath + "supplier" + extension)
				.setOutputPartKey(hashSupplier).add(projectionSupplier);
		_queryBuilder.add(relationSupplier);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashRNS = Arrays.asList(2);

		final ProjectOperator projectionRNS = new ProjectOperator(new int[] {
				0, 1, 2 });

		final EquiJoinComponent R_N_Sjoin = new EquiJoinComponent(R_Njoin,
				relationSupplier).setOutputPartKey(hashRNS).add(projectionRNS);
		_queryBuilder.add(R_N_Sjoin);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashLineitem = Arrays.asList(1);

		final ProjectOperator projectionLineitem = new ProjectOperator(
				new int[] { 0, 2, 5, 6 });

		final DataSourceComponent relationLineitem = new DataSourceComponent(
				"LINEITEM", dataPath + "lineitem" + extension)
				.setOutputPartKey(hashLineitem).add(projectionLineitem);
		_queryBuilder.add(relationLineitem);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashRNSL = Arrays.asList(0, 2);

		final ProjectOperator projectionRNSL = new ProjectOperator(new int[] {
				0, 1, 3, 4, 5 });

		final EquiJoinComponent R_N_S_Ljoin = new EquiJoinComponent(R_N_Sjoin,
				relationLineitem).setOutputPartKey(hashRNSL)
				.add(projectionRNSL);
		_queryBuilder.add(R_N_S_Ljoin);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashCustomer = Arrays.asList(0);

		final ProjectOperator projectionCustomer = new ProjectOperator(
				new int[] { 0, 3 });

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
		final List<Integer> hashCO = Arrays.asList(0, 1);

		final ProjectOperator projectionCO = new ProjectOperator(new int[] { 1,
				2 });

		final EquiJoinComponent C_Ojoin = new EquiJoinComponent(
				relationCustomer, relationOrders).setOutputPartKey(hashCO).add(
				projectionCO);
		_queryBuilder.add(C_Ojoin);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashRNSLCO = Arrays.asList(0);

		final ProjectOperator projectionRNSLCO = new ProjectOperator(new int[] {
				1, 3, 4 });

		final EquiJoinComponent R_N_S_L_C_Ojoin = new EquiJoinComponent(
				R_N_S_Ljoin, C_Ojoin).setOutputPartKey(hashRNSLCO).add(
				projectionRNSLCO);
		_queryBuilder.add(R_N_S_L_C_Ojoin);

		// -------------------------------------------------------------------------------------
		// set up aggregation function on a separate StormComponent(Bolt)

		final ValueExpression<Double> substract = new Subtraction(
				new ValueSpecification(_doubleConv, 1.0), new ColumnReference(
						_doubleConv, 2));
		// extendedPrice*(1-discount)
		final ValueExpression<Double> product = new Multiplication(
				new ColumnReference(_doubleConv, 1), substract);

		final AggregateOperator aggOp = new AggregateAvgOperator(product, conf)
				.setGroupByColumns(Arrays.asList(0));
		OperatorComponent oc = new OperatorComponent(R_N_S_L_C_Ojoin,
				"FINAL_RESULT").add(aggOp);
		_queryBuilder.add(oc);

		// -------------------------------------------------------------------------------------
	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
