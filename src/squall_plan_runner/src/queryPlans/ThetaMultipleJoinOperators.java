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
import operators.SelectOperator;
import org.apache.log4j.Logger;

import predicates.AndPredicate;
import predicates.ComparisonPredicate;

public class ThetaMultipleJoinOperators {

	private static Logger LOG = Logger.getLogger(ThetaMultipleJoinOperators.class);

	private QueryPlan _queryPlan = new QueryPlan();

	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final NumericConversion<Integer> _intConv = new IntegerConversion();

	public ThetaMultipleJoinOperators(String dataPath, String extension, Map conf) {

		// -------------------------------------------------------------------------------------
		List<Integer> hashLineitem = Arrays.asList(0);

		ProjectOperator projectionLineitem = new ProjectOperator(
				new int[] { 0, 1, 2 ,5, 6 });

		DataSourceComponent relationLineitem = new DataSourceComponent(
				"LINEITEM", 
                                dataPath + "lineitem" + extension,
                                _queryPlan).setHashIndexes(hashLineitem)
				           .addOperator(projectionLineitem);

		// -------------------------------------------------------------------------------------
		List<Integer> hashOrders= Arrays.asList(0);

		ProjectOperator projectionOrders = new ProjectOperator(
				new int[] { 0, 3 });

		DataSourceComponent relationOrders = new DataSourceComponent(
				"ORDERS", 
                                dataPath + "orders" + extension,
                                _queryPlan).setHashIndexes(hashOrders)
				           .addOperator(projectionOrders);

		// -------------------------------------------------------------------------------------
		List<Integer> hashSupplier= Arrays.asList(0);

		ProjectOperator projectionSupplier = new ProjectOperator(
				new int[] { 0 });

		DataSourceComponent relationSupplier= new DataSourceComponent(
				"SUPPLIER",
                                dataPath + "supplier" + extension,
				_queryPlan).setHashIndexes(hashSupplier)
				           .addOperator(projectionSupplier);

		//-------------------------------------------------------------------------------------

		List<Integer> hashPartsSupp= Arrays.asList(0);

		ProjectOperator projectionPartsSupp = new ProjectOperator(
				new int[] { 0 ,1 , 2 });
		
		/*ColumnReference colQty = new ColumnReference(_intConv, 2);
		ValueSpecification val9990 = new ValueSpecification(_intConv, 9990);
		SelectionOperator select = new SelectionOperator(new ComparisonPredicate(ComparisonPredicate.GREATER_OP, colQty, val9990));
                */
		DataSourceComponent relationPartsupp= new DataSourceComponent(
				"PARTSUPP",
                                dataPath + "partsupp" + extension,
				_queryPlan).setHashIndexes(hashPartsSupp)
				           .addOperator(projectionPartsSupp);

		//-------------------------------------------------------------------------------------

		ColumnReference colRefLineItem = new ColumnReference(_intConv, 0);
		ColumnReference colRefOrders = new ColumnReference(_intConv, 0);
		ComparisonPredicate predL_O1 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, colRefLineItem, colRefOrders);
		
		ColumnReference colRefLineItemExtPrice = new ColumnReference(_doubleConv, 3);
		ColumnReference colRefOrdersTotalPrice = new ColumnReference(_doubleConv, 1);
		ValueSpecification val10 = new ValueSpecification(_doubleConv, 10.0);
		Multiplication mult = new Multiplication(_doubleConv, val10, colRefLineItemExtPrice);
		ComparisonPredicate predL_O2 = new ComparisonPredicate(ComparisonPredicate.LESS_OP, mult, colRefOrdersTotalPrice);
		
		AndPredicate predL_O = new AndPredicate(predL_O1, predL_O2);
		
		ThetaJoinComponent LINEITEMS_ORDERSjoin = new ThetaJoinComponent(
				relationLineitem,
				relationOrders,
				_queryPlan).setJoinPredicate(predL_O)
				           .addOperator(new ProjectOperator(new int[]{1, 2, 3,4}));
		//-------------------------------------------------------------------------------------
		
                SelectOperator selectionPartSupp = new SelectOperator(
                    new ComparisonPredicate(
                        ComparisonPredicate.GREATER_OP,
                        new ColumnReference(_intConv, 2),
                        new ValueSpecification(_intConv, 9990)
                    ));
        
                ColumnReference colRefSupplier = new ColumnReference(_intConv, 0);
		ColumnReference colRefPartSupp = new ColumnReference(_intConv, 1);
                ComparisonPredicate predS_P = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, colRefSupplier, colRefPartSupp);
        

		ThetaJoinComponent SUPPLIER_PARTSUPPjoin = new ThetaJoinComponent(
				relationSupplier,
				relationPartsupp,
				_queryPlan).setJoinPredicate(predS_P)
				           .addOperator(new ProjectOperator(new int[]{0,1,3}))
				           .addOperator(selectionPartSupp);
		
		//-------------------------------------------------------------------------------------
		
		// set up aggregation function on the StormComponent(Bolt) where join is performed

		//1 - discount
		ValueExpression<Double> substract = new Subtraction(
				_doubleConv,
				new ValueSpecification(_doubleConv, 1.0),
				new ColumnReference(_doubleConv, 3));
		//extendedPrice*(1-discount)
		ValueExpression<Double> product = new Multiplication(
				_doubleConv,
				new ColumnReference(_doubleConv, 2),
				substract);
		AggregateOperator agg = new AggregateSumOperator(_doubleConv, product, conf);
		
	
		ColumnReference colRefL_OPartKey = new ColumnReference(_intConv, 0);
		ColumnReference colRefS_PPartKey = new ColumnReference(_intConv, 1);
		ColumnReference colRefL_OSupKey = new ColumnReference(_intConv, 1);
		ColumnReference colRefS_PSupKey = new ColumnReference(_intConv, 0);
		ComparisonPredicate predL_P1 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, colRefL_OPartKey, colRefS_PPartKey);
		ComparisonPredicate predL_P2 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, colRefL_OSupKey, colRefS_PSupKey);
		AndPredicate predL_P = new AndPredicate(predL_P1, predL_P2);
		


		ThetaJoinComponent LINEITEMS_ORDERS_SUPPLIER_PARTSUPPjoin = new ThetaJoinComponent(
				LINEITEMS_ORDERSjoin,
				SUPPLIER_PARTSUPPjoin,
				_queryPlan).setJoinPredicate(predL_P)
				           .addOperator(new ProjectOperator(new int[]{0,1, 2, 3}))
				           .addOperator(agg);
		

		//-------------------------------------------------------------------------------------

		AggregateOperator overallAgg =
			new AggregateSumOperator(_doubleConv, new ColumnReference(_doubleConv, 0), conf); //WHAT COLUMN HERE??

		_queryPlan.setOverallAggregation(overallAgg);


	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}