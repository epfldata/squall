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
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.DateSum;
import plan_runner.expressions.Multiplication;
import plan_runner.expressions.Subtraction;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.BetweenPredicate;
import plan_runner.predicates.ComparisonPredicate;

public class TPCH5Plan {
	private static Logger LOG = Logger.getLogger(TPCH5Plan.class);

	private static final TypeConversion<Date> _dc = new DateConversion();
	private static final TypeConversion<String> _sc = new StringConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();

	private final QueryPlan _queryPlan = new QueryPlan();

	// query variables
	private static Date _date1, _date2;
	private static final String REGION_NAME = "ASIA";

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

	public TPCH5Plan(String dataPath, String extension, Map conf) {
		computeDates();

		// -------------------------------------------------------------------------------------
		final List<Integer> hashRegion = Arrays.asList(0);

		final SelectOperator selectionRegion = new SelectOperator(new ComparisonPredicate(
				new ColumnReference(_sc, 1), new ValueSpecification(_sc, REGION_NAME)));

		final ProjectOperator projectionRegion = new ProjectOperator(new int[] { 0 });

		final DataSourceComponent relationRegion = new DataSourceComponent("REGION", dataPath
				+ "region" + extension, _queryPlan).setHashIndexes(hashRegion)
				.addOperator(selectionRegion).addOperator(projectionRegion);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashNation = Arrays.asList(2);

		final ProjectOperator projectionNation = new ProjectOperator(new int[] { 0, 1, 2 });

		final DataSourceComponent relationNation = new DataSourceComponent("NATION", dataPath
				+ "nation" + extension, _queryPlan).setHashIndexes(hashNation).addOperator(
				projectionNation);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashRN = Arrays.asList(0);

		final ProjectOperator projectionRN = new ProjectOperator(new int[] { 1, 2 });

		final EquiJoinComponent R_Njoin = new EquiJoinComponent(relationRegion, relationNation,
				_queryPlan).setHashIndexes(hashRN).addOperator(projectionRN);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashSupplier = Arrays.asList(1);

		final ProjectOperator projectionSupplier = new ProjectOperator(new int[] { 0, 3 });

		final DataSourceComponent relationSupplier = new DataSourceComponent("SUPPLIER", dataPath
				+ "supplier" + extension, _queryPlan).setHashIndexes(hashSupplier).addOperator(
				projectionSupplier);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashRNS = Arrays.asList(2);

		final ProjectOperator projectionRNS = new ProjectOperator(new int[] { 0, 1, 2 });

		final EquiJoinComponent R_N_Sjoin = new EquiJoinComponent(R_Njoin, relationSupplier,
				_queryPlan).setHashIndexes(hashRNS).addOperator(projectionRNS);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashLineitem = Arrays.asList(1);

		final ProjectOperator projectionLineitem = new ProjectOperator(new int[] { 0, 2, 5, 6 });

		final DataSourceComponent relationLineitem = new DataSourceComponent("LINEITEM", dataPath
				+ "lineitem" + extension, _queryPlan).setHashIndexes(hashLineitem).addOperator(
				projectionLineitem);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashRNSL = Arrays.asList(0, 2);

		final ProjectOperator projectionRNSL = new ProjectOperator(new int[] { 0, 1, 3, 4, 5 });

		final EquiJoinComponent R_N_S_Ljoin = new EquiJoinComponent(R_N_Sjoin, relationLineitem,
				_queryPlan).setHashIndexes(hashRNSL).addOperator(projectionRNSL);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashCustomer = Arrays.asList(0);

		final ProjectOperator projectionCustomer = new ProjectOperator(new int[] { 0, 3 });

		final DataSourceComponent relationCustomer = new DataSourceComponent("CUSTOMER", dataPath
				+ "customer" + extension, _queryPlan).setHashIndexes(hashCustomer).addOperator(
				projectionCustomer);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashOrders = Arrays.asList(1);

		final SelectOperator selectionOrders = new SelectOperator(new BetweenPredicate(
				new ColumnReference(_dc, 4), true, new ValueSpecification(_dc, _date1), false,
				new ValueSpecification(_dc, _date2)));

		final ProjectOperator projectionOrders = new ProjectOperator(new int[] { 0, 1 });

		final DataSourceComponent relationOrders = new DataSourceComponent("ORDERS", dataPath
				+ "orders" + extension, _queryPlan).setHashIndexes(hashOrders)
				.addOperator(selectionOrders).addOperator(projectionOrders);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashCO = Arrays.asList(0, 1);

		final ProjectOperator projectionCO = new ProjectOperator(new int[] { 1, 2 });

		final EquiJoinComponent C_Ojoin = new EquiJoinComponent(relationCustomer, relationOrders,
				_queryPlan).setHashIndexes(hashCO).addOperator(projectionCO);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashRNSLCO = Arrays.asList(0);

		final ProjectOperator projectionRNSLCO = new ProjectOperator(new int[] { 1, 3, 4 });

		final EquiJoinComponent R_N_S_L_C_Ojoin = new EquiJoinComponent(R_N_S_Ljoin, C_Ojoin,
				_queryPlan).setHashIndexes(hashRNSLCO).addOperator(projectionRNSLCO);

		// -------------------------------------------------------------------------------------
		// set up aggregation function on a separate StormComponent(Bolt)

		final ValueExpression<Double> substract = new Subtraction(new ValueSpecification(
				_doubleConv, 1.0), new ColumnReference(_doubleConv, 2));
		// extendedPrice*(1-discount)
		final ValueExpression<Double> product = new Multiplication(new ColumnReference(_doubleConv,
				1), substract);

		final AggregateOperator aggOp = new AggregateSumOperator(product, conf)
				.setGroupByColumns(Arrays.asList(0));
		new OperatorComponent(R_N_S_L_C_Ojoin, "FINAL_RESULT", _queryPlan).addOperator(aggOp);

		// -------------------------------------------------------------------------------------
	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}