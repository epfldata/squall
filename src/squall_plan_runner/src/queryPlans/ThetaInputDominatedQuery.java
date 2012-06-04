/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans;

import schema.TPCH_Schema;
import components.DataSourceComponent;
import components.ThetaJoinComponent;
import conversion.DateConversion;
import conversion.DoubleConversion;
import conversion.IntegerConversion;
import conversion.NumericConversion;
import conversion.StringConversion;
import conversion.TypeConversion;
import expressions.ColumnReference;
import expressions.IntegerYearFromDate;
import expressions.Multiplication;
import expressions.Subtraction;
import expressions.ValueExpression;
import expressions.ValueSpecification;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import operators.AggregateOperator;
import operators.AggregateSumOperator;
import operators.ProjectOperator;
import operators.SelectOperator;
import org.apache.log4j.Logger;
import predicates.AndPredicate;
import predicates.BetweenPredicate;
import predicates.ComparisonPredicate;
import predicates.OrPredicate;
import queryPlans.QueryPlan;

public class ThetaInputDominatedQuery {

	private static Logger LOG = Logger.getLogger(ThetaInputDominatedQuery.class);

	private QueryPlan _queryPlan = new QueryPlan();

	private static final String _date1Str = "1995-01-01";
	private static final String _date2Str = "1996-12-31";
	private static final String _firstCountryName = "FRANCE";
	private static final String _secondCountryName = "GERMANY";

	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final TypeConversion<String> _sc = new StringConversion();
	private static final IntegerConversion _ic = new IntegerConversion();
	private static final Date _date1 = _dateConv.fromString(_date1Str);
	private static final Date _date2 = _dateConv.fromString(_date2Str);

	public ThetaInputDominatedQuery(String dataPath, String extension, Map conf) {

		// -------------------------------------------------------------------------------------
		List<Integer> hashLineitem = Arrays.asList(0);

		ProjectOperator projectionLineitem = new ProjectOperator(
				new int[] { 0, 5, 6 });

		DataSourceComponent relationLineitem = new DataSourceComponent(
				"LINEITEM", dataPath + "lineitem" + extension,
				TPCH_Schema.lineitem, _queryPlan).setHashIndexes(hashLineitem)
				.addOperator(projectionLineitem);

		// -------------------------------------------------------------------------------------
		List<Integer> hashOrders= Arrays.asList(1);

		ProjectOperator projectionOrders = new ProjectOperator(
				new int[] { 0, 3 });

		DataSourceComponent relationOrders = new DataSourceComponent(
				"ORDERS", dataPath + "orders" + extension,
				TPCH_Schema.orders, _queryPlan).setHashIndexes(hashOrders)
				.addOperator(projectionOrders);

		//-------------------------------------------------------------------------------------

		// set up aggregation function on the StormComponent(Bolt) where join is performed

		//1 - discount
		ValueExpression<Double> substract = new Subtraction(
				_doubleConv,
				new ValueSpecification(_doubleConv, 1.0),
				new ColumnReference(_doubleConv, 1));
		//extendedPrice*(1-discount)
		ValueExpression<Double> product = new Multiplication(
				_doubleConv,
				new ColumnReference(_doubleConv, 0),
				substract);
		AggregateOperator agg = new AggregateSumOperator(_doubleConv, product, conf);
		

		
		
		ColumnReference colLineItems = new ColumnReference(_ic, 0);
		ColumnReference colOrders = new ColumnReference(_ic, 0);
		ComparisonPredicate pred1 = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colLineItems, colOrders);
		
		ValueSpecification value10 = new ValueSpecification(_doubleConv,10.0);
		ColumnReference colLineItemsInequality = new ColumnReference(_doubleConv, 1);
		Multiplication mult = new Multiplication(_doubleConv, value10,colLineItemsInequality);
		ColumnReference colOrdersInequality = new ColumnReference(_doubleConv, 1);
		//ComparisonPredicate pred2 = new ComparisonPredicate(ComparisonPredicate.LESS_OP,mult, colOrdersInequality);
		ComparisonPredicate pred2 = new ComparisonPredicate(ComparisonPredicate.LESS_OP, mult, colOrdersInequality);
		
		AndPredicate overallPred= new AndPredicate(pred1, pred2);
		
		ThetaJoinComponent LINEITEMS_ORDERSjoin = new ThetaJoinComponent(
				relationLineitem,
				relationOrders,
				_queryPlan)
				.setJoinPredicate(overallPred)
		//.setJoinPredicate(pred2)
				.addOperator(new ProjectOperator(new int[]{1, 2, 4}))
				.addOperator(agg)
				;
		//-------------------------------------------------------------------------------------


		//-------------------------------------------------------------------------------------

		AggregateOperator overallAgg =
			new AggregateSumOperator(_doubleConv, new ColumnReference(_doubleConv, 1), conf);

		_queryPlan.setOverallAggregation(overallAgg);


	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}