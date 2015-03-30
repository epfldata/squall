package ch.epfl.data.squall.query_plans.debug;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.conversion.DateConversion;
import ch.epfl.data.squall.conversion.DoubleConversion;
import ch.epfl.data.squall.conversion.NumericConversion;
import ch.epfl.data.squall.conversion.StringConversion;
import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;

public class TPCH3L1Plan extends QueryPlan {
	private static Logger LOG = Logger.getLogger(TPCH3L1Plan.class);

	private static final String _customerMktSegment = "BUILDING";
	private static final String _dateStr = "1995-03-15";

	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final TypeConversion<String> _sc = new StringConversion();
	private static final Date _date = _dateConv.fromString(_dateStr);

	private final QueryBuilder _queryBuilder = new QueryBuilder();

	public TPCH3L1Plan(String dataPath, String extension, Map conf) {

		// -------------------------------------------------------------------------------------
		final List<Integer> hashCustomer = Arrays.asList(0);

		final SelectOperator selectionCustomer = new SelectOperator(
				new ComparisonPredicate(new ColumnReference(_sc, 6),
						new ValueSpecification(_sc, _customerMktSegment)));

		final ProjectOperator projectionCustomer = new ProjectOperator(
				new int[] { 0 });

		DataSourceComponent relationCustomer = new DataSourceComponent(
				"CUSTOMER", dataPath + "customer" + extension)
				.setOutputPartKey(hashCustomer).add(selectionCustomer)
				.add(projectionCustomer).setPrintOut(false);
		_queryBuilder.add(relationCustomer);

		// -------------------------------------------------------------------------------------
		final List<Integer> hashOrders = Arrays.asList(1);

		final SelectOperator selectionOrders = new SelectOperator(
				new ComparisonPredicate(ComparisonPredicate.LESS_OP,
						new ColumnReference(_dateConv, 4),
						new ValueSpecification(_dateConv, _date)));

		final ProjectOperator projectionOrders = new ProjectOperator(new int[] {
				0, 1, 4, 7 });

		DataSourceComponent relationOrders = new DataSourceComponent("ORDERS",
				dataPath + "orders" + extension).setOutputPartKey(hashOrders)
				.add(selectionOrders).add(projectionOrders).setPrintOut(false);
		_queryBuilder.add(relationOrders);

		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
