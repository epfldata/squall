/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans.debug;

import schema.TPCH_Schema;
import components.DataSourceComponent;
import components.JoinComponent;
import conversion.DateConversion;
import conversion.DoubleConversion;
import conversion.NumericConversion;
import conversion.StringConversion;
import conversion.TypeConversion;
import expressions.ColumnReference;
import expressions.ValueSpecification;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import operators.ProjectionOperator;
import operators.SelectionOperator;
import org.apache.log4j.Logger;
import predicates.ComparisonPredicate;
import queryPlans.QueryPlan;

public class TPCH3L23Plan {
	private static Logger LOG = Logger.getLogger(TPCH3L23Plan.class);

	private static final String _customerMktSegment = "BUILDING";
	private static final String _dateStr = "1995-03-15";

	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final TypeConversion<String> _sc = new StringConversion();
	private static final Date _date = _dateConv.fromString(_dateStr);

	private QueryPlan _queryPlan = new QueryPlan();

	public TPCH3L23Plan(String dataPath, String extension, Map conf){

		//-------------------------------------------------------------------------------------
		List<Integer> hashCustomer = Arrays.asList(0);

		SelectionOperator selectionCustomer = new SelectionOperator(
				new ComparisonPredicate(
					new ColumnReference(_sc, 6),
					new ValueSpecification(_sc, _customerMktSegment)
					));

		ProjectionOperator projectionCustomer = new ProjectionOperator(new int[]{0});

		DataSourceComponent relationCustomer = new DataSourceComponent(
				"CUSTOMER",
				dataPath + "customer" + extension,
				TPCH_Schema.customer,
				_queryPlan).setHashIndexes(hashCustomer)
			.setSelection(selectionCustomer)
			.setProjection(projectionCustomer);

		//-------------------------------------------------------------------------------------
		List<Integer> hashOrders = Arrays.asList(1);

		SelectionOperator selectionOrders = new SelectionOperator(
				new ComparisonPredicate(
					ComparisonPredicate.LESS_OP,
					new ColumnReference(_dateConv, 4),
					new ValueSpecification(_dateConv, _date)
					));

		ProjectionOperator projectionOrders = new ProjectionOperator(new int[]{0, 1, 4, 7});

		DataSourceComponent relationOrders = new DataSourceComponent(
				"ORDERS",
				dataPath + "orders" + extension,
				TPCH_Schema.orders,
				_queryPlan).setHashIndexes(hashOrders)
			.setSelection(selectionOrders)
			.setProjection(projectionOrders);

		//-------------------------------------------------------------------------------------
		JoinComponent C_Ojoin = new JoinComponent(
				relationCustomer,
				relationOrders,
				_queryPlan).setProjection(new ProjectionOperator(new int[]{1, 2, 3}))
			.setHashIndexes(Arrays.asList(0));

		//-------------------------------------------------------------------------------------
		List<Integer> hashLineitem = Arrays.asList(0);

		SelectionOperator selectionLineitem = new SelectionOperator(
				new ComparisonPredicate(
					ComparisonPredicate.GREATER_OP,
					new ColumnReference(_dateConv, 10),
					new ValueSpecification(_dateConv, _date)
					));

		ProjectionOperator projectionLineitem = new ProjectionOperator(new int[]{0, 5, 6});

		DataSourceComponent relationLineitem = new DataSourceComponent(
				"LINEITEM",
				dataPath + "lineitem" + extension,
				TPCH_Schema.lineitem,
				_queryPlan).setHashIndexes(hashLineitem)
			.setSelection(selectionLineitem)
			.setProjection(projectionLineitem).setPrintOut(false);

		//-------------------------------------------------------------------------------------

	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}
