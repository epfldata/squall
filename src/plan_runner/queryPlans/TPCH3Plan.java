/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package plan_runner.queryPlans;

import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.conversion.DateConversion;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.Multiplication;
import plan_runner.expressions.Subtraction;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import org.apache.log4j.Logger;
import plan_runner.predicates.ComparisonPredicate;

public class TPCH3Plan {
	private static Logger LOG = Logger.getLogger(TPCH3Plan.class);

	private static final String _customerMktSegment = "BUILDING";
	private static final String _dateStr = "1995-03-15";

	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final TypeConversion<String> _sc = new StringConversion();
	private static final Date _date = _dateConv.fromString(_dateStr);

	private QueryPlan _queryPlan = new QueryPlan();

	public TPCH3Plan(String dataPath, String extension, Map conf){

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
				_queryPlan).setHashIndexes(hashCustomer)
                                           .addOperator(selectionCustomer)
                                           .addOperator(projectionCustomer);

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
				_queryPlan).setHashIndexes(hashOrders)
                                           .addOperator(selectionOrders)
                                           .addOperator(projectionOrders);

		//-------------------------------------------------------------------------------------
		EquiJoinComponent C_Ojoin = new EquiJoinComponent(
				relationCustomer,
				relationOrders,
				_queryPlan).addOperator(new ProjectOperator(new int[]{1, 2, 3}))
                                           .setHashIndexes(Arrays.asList(0));

		//-------------------------------------------------------------------------------------
		List<Integer> hashLineitem = Arrays.asList(0);

		SelectOperator selectionLineitem = new SelectOperator(
				new ComparisonPredicate(
					ComparisonPredicate.GREATER_OP,
					new ColumnReference(_dateConv, 10),
					new ValueSpecification(_dateConv, _date)
					));

		ProjectOperator projectionLineitem = new ProjectOperator(new int[]{0, 5, 6});

		DataSourceComponent relationLineitem = new DataSourceComponent(
				"LINEITEM",
				dataPath + "lineitem" + extension,
				_queryPlan).setHashIndexes(hashLineitem)
                                           .addOperator(selectionLineitem)
                                           .addOperator(projectionLineitem);

		//-------------------------------------------------------------------------------------
		// set up aggregation function on the StormComponent(Bolt) where join is performed

		//1 - discount
		ValueExpression<Double> substract = new Subtraction(
				_doubleConv,
				new ValueSpecification(_doubleConv, 1.0),
				new ColumnReference(_doubleConv, 4));
		//extendedPrice*(1-discount)
		ValueExpression<Double> product = new Multiplication(
				_doubleConv,
				new ColumnReference(_doubleConv, 3),
				substract);
		AggregateOperator agg = new AggregateSumOperator(_doubleConv, product, conf)
			.setGroupByColumns(Arrays.asList(0, 1, 2));

		EquiJoinComponent C_O_Ljoin = new EquiJoinComponent(
				C_Ojoin,
				relationLineitem,
				_queryPlan).addOperator(agg);

		//-------------------------------------------------------------------------------------

		AggregateOperator overallAgg =
			new AggregateSumOperator(_doubleConv, new ColumnReference(_doubleConv, 1), conf)
			.setGroupByColumns(Arrays.asList(0));

		_queryPlan.setOverallAggregation(overallAgg);

	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}
