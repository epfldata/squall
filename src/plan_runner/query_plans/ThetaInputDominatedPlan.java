package plan_runner.query_plans;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.ThetaJoinStaticComponent;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.Multiplication;
import plan_runner.expressions.Subtraction;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.predicates.AndPredicate;
import plan_runner.predicates.ComparisonPredicate;

public class ThetaInputDominatedPlan {

	private static Logger LOG = Logger.getLogger(ThetaInputDominatedPlan.class);

	private QueryPlan _queryPlan = new QueryPlan();

	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final IntegerConversion _ic = new IntegerConversion();

        /*
         *  SELECT SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT))
         *  FROM LINEITEM, ORDERS
         *  WHERE LINEITEM.ORDERKEY = ORDERS.ORDERKEY AND
         *  ORDERS.TOTALPRICE > 10*LINEITEM.EXTENDEDPRICE
         */
	public ThetaInputDominatedPlan(String dataPath, String extension, Map conf) {

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
				new ValueSpecification(_doubleConv, 1.0),
				new ColumnReference(_doubleConv, 1));
		//extendedPrice*(1-discount)
		ValueExpression<Double> product = new Multiplication(
				new ColumnReference(_doubleConv, 0),
				substract);
		AggregateOperator agg = new AggregateSumOperator(product, conf);
		
		ColumnReference colLineItems = new ColumnReference(_ic, 0);
		ColumnReference colOrders = new ColumnReference(_ic, 0);
		ComparisonPredicate pred1 = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colLineItems, colOrders);
		
		ValueSpecification value10 = new ValueSpecification(_doubleConv,10.0);
		ColumnReference colLineItemsInequality = new ColumnReference(_doubleConv, 1);
		Multiplication mult = new Multiplication(value10, colLineItemsInequality);
		ColumnReference colOrdersInequality = new ColumnReference(_doubleConv, 1);
		//ComparisonPredicate pred2 = new ComparisonPredicate(ComparisonPredicate.LESS_OP,mult, colOrdersInequality);
		ComparisonPredicate pred2 = new ComparisonPredicate(ComparisonPredicate.LESS_OP, mult, colOrdersInequality);
		
		AndPredicate overallPred= new AndPredicate(pred1, pred2);
		
		ThetaJoinStaticComponent LINEITEMS_ORDERSjoin = new ThetaJoinStaticComponent(
				relationLineitem,
				relationOrders,
				_queryPlan).setJoinPredicate(overallPred)
                                         //.setJoinPredicate(pred2)
				           .addOperator(new ProjectOperator(new int[]{1, 2, 4}))
				           .addOperator(agg);
		
		//-------------------------------------------------------------------------------------

	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}