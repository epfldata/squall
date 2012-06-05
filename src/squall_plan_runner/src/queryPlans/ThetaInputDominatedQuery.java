/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans;

import components.DataSourceComponent;
import components.ThetaJoinComponent;
import conversion.DoubleConversion;
import conversion.IntegerConversion;
import conversion.NumericConversion;
import expressions.ColumnReference;
import expressions.Multiplication;
import expressions.Subtraction;
import expressions.ValueExpression;
import expressions.ValueSpecification;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import operators.AggregateOperator;
import operators.AggregateSumOperator;
import operators.ProjectOperator;
import org.apache.log4j.Logger;
import predicates.AndPredicate;
import predicates.ComparisonPredicate;

public class ThetaInputDominatedQuery {

	private static Logger LOG = Logger.getLogger(ThetaInputDominatedQuery.class);

	private QueryPlan _queryPlan = new QueryPlan();

	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final IntegerConversion _ic = new IntegerConversion();

	public ThetaInputDominatedQuery(String dataPath, String extension, Map conf) {

		// -------------------------------------------------------------------------------------
		List<Integer> hashLineitem = Arrays.asList(0);

		ProjectOperator projectionLineitem = new ProjectOperator(
				new int[] { 0, 5, 6 });

		DataSourceComponent relationLineitem = new DataSourceComponent(
				"LINEITEM",
                                dataPath + "lineitem" + extension,
				_queryPlan).setHashIndexes(hashLineitem)
				           .addOperator(projectionLineitem);

		// -------------------------------------------------------------------------------------
		List<Integer> hashOrders= Arrays.asList(1);

		ProjectOperator projectionOrders = new ProjectOperator(
				new int[] { 0, 3 });

		DataSourceComponent relationOrders = new DataSourceComponent(
				"ORDERS",
                                dataPath + "orders" + extension,
				_queryPlan).setHashIndexes(hashOrders)
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
				_queryPlan).setJoinPredicate(overallPred)
                                         //.setJoinPredicate(pred2)
				           .addOperator(new ProjectOperator(new int[]{1, 2, 4}))
				           .addOperator(agg);
		
		//-------------------------------------------------------------------------------------

		AggregateOperator overallAgg =
			new AggregateSumOperator(_doubleConv, new ColumnReference(_doubleConv, 1), conf);

		_queryPlan.setOverallAggregation(overallAgg);


	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}