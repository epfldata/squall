package ch.epfl.data.plan_runner.query_plans.debug;

import java.util.Arrays;
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
import ch.epfl.data.plan_runner.expressions.ValueSpecification;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.operators.SelectOperator;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import ch.epfl.data.plan_runner.query_plans.QueryPlan;

public class TPCH3L23Plan extends QueryPlan {
	private static Logger LOG = Logger.getLogger(TPCH3L23Plan.class);

	private static final String _customerMktSegment = "BUILDING";
	private static final String _dateStr = "1995-03-15";

	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final TypeConversion<String> _sc = new StringConversion();
	private static final Date _date = _dateConv.fromString(_dateStr);

	private final QueryBuilder _queryBuilder = new QueryBuilder();

	public TPCH3L23Plan(String dataPath, String extension, Map conf) {

		// -------------------------------------------------------------------------------------
		final List<Integer> hashCustomer = Arrays.asList(0);

		final SelectOperator selectionCustomer = new SelectOperator(
				new ComparisonPredicate(new ColumnReference(_sc, 6),
						new ValueSpecification(_sc, _customerMktSegment)));

		final ProjectOperator projectionCustomer = new ProjectOperator(
				new int[] { 0 });

		final DataSourceComponent relationCustomer = new DataSourceComponent(
				"CUSTOMER", dataPath + "customer" + extension)
				.setOutputPartKey(hashCustomer).add(selectionCustomer)
				.add(projectionCustomer);
		_queryBuilder.add(relationCustomer);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashOrders = Arrays.asList(1);

		final SelectOperator selectionOrders = new SelectOperator(
				new ComparisonPredicate(ComparisonPredicate.LESS_OP,
						new ColumnReference(_dateConv, 4),
						new ValueSpecification(_dateConv, _date)));

		final ProjectOperator projectionOrders = new ProjectOperator(new int[] {
				0, 1, 4, 7 });

		final DataSourceComponent relationOrders = new DataSourceComponent(
				"ORDERS", dataPath + "orders" + extension)
				.setOutputPartKey(hashOrders).add(selectionOrders)
				.add(projectionOrders);
		_queryBuilder.add(relationOrders);

		final EquiJoinComponent joinCustOrders = new EquiJoinComponent(
				relationCustomer, relationOrders).add(
				new ProjectOperator(new int[] { 1, 2, 3 })).setOutputPartKey(
				Arrays.asList(0));
		_queryBuilder.add(joinCustOrders);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashLineitem = Arrays.asList(0);

		final SelectOperator selectionLineitem = new SelectOperator(
				new ComparisonPredicate(ComparisonPredicate.GREATER_OP,
						new ColumnReference(_dateConv, 10),
						new ValueSpecification(_dateConv, _date)));

		final ProjectOperator projectionLineitem = new ProjectOperator(
				new int[] { 0, 5, 6 });

		final DataSourceComponent relationLineitem = new DataSourceComponent(
				"LINEITEM", dataPath + "lineitem" + extension)
				.setOutputPartKey(hashLineitem).add(selectionLineitem)
				.add(projectionLineitem).setPrintOut(false);
		_queryBuilder.add(relationLineitem);

		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
