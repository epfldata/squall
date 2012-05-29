/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans.debug;

import schema.TPCH_Schema;
import components.DataSourceComponent;
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
import operators.ProjectOperator;
import operators.SelectOperator;
import org.apache.log4j.Logger;
import predicates.ComparisonPredicate;
import queryPlans.QueryPlan;

public class TPCH3L1Plan {
	private static Logger LOG = Logger.getLogger(TPCH3L1Plan.class);

	private static final String _customerMktSegment = "BUILDING";
	private static final String _dateStr = "1995-03-15";

	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final TypeConversion<String> _sc = new StringConversion();
	private static final Date _date = _dateConv.fromString(_dateStr);

	private QueryPlan _queryPlan = new QueryPlan();

	public TPCH3L1Plan(String dataPath, String extension, Map conf){

		//-------------------------------------------------------------------------------------
		List<Integer> hashCustomer = Arrays.asList(0);

		SelectOperator selectionCustomer = new SelectOperator(
				new ComparisonPredicate(
					new ColumnReference(_sc, 6),
					new ValueSpecification(_sc, _customerMktSegment)
					));

		ProjectOperator projectionCustomer = new ProjectOperator(new int[]{0});

		DataSourceComponent relationCustomer = new DataSourceComponent(
				"CUSTOMER",
				dataPath + "customer" + extension,
				TPCH_Schema.customer,
				_queryPlan).setHashIndexes(hashCustomer)
                                           .addOperator(selectionCustomer)
                                           .addOperator(projectionCustomer)
                                           .setPrintOut(false);

		//-------------------------------------------------------------------------------------
		List<Integer> hashOrders = Arrays.asList(1);

		SelectOperator selectionOrders = new SelectOperator(
				new ComparisonPredicate(
					ComparisonPredicate.LESS_OP,
					new ColumnReference(_dateConv, 4),
					new ValueSpecification(_dateConv, _date)
					));

		ProjectOperator projectionOrders = new ProjectOperator(new int[]{0, 1, 4, 7});

		DataSourceComponent relationOrders = new DataSourceComponent(
				"ORDERS",
				dataPath + "orders" + extension,
				TPCH_Schema.orders,
				_queryPlan).setHashIndexes(hashOrders)
                                           .addOperator(selectionOrders)
                                           .addOperator(projectionOrders)
                                           .setPrintOut(false);

		//-------------------------------------------------------------------------------------
		

	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}
