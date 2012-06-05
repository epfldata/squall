/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans;

import components.DataSourceComponent;
import components.ThetaJoinComponent;
import conversion.DoubleConversion;
import conversion.NumericConversion;
import expressions.ColumnReference;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import operators.AggregateOperator;
import operators.AggregateSumOperator;
import operators.ProjectOperator;
import org.apache.log4j.Logger;


public class ThetaOutputDominatedPlan {

	private static Logger LOG = Logger.getLogger(ThetaOutputDominatedPlan.class);

	private QueryPlan _queryPlan = new QueryPlan();

	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();

        /*
         * SELECT SUM(SUPPLIER.SUPPKEY)
         * FROM SUPPLIER, NATION
         */
	public ThetaOutputDominatedPlan(String dataPath, String extension, Map conf) {

		// -------------------------------------------------------------------------------------
		List<Integer> hashSupplier = Arrays.asList(0);

		ProjectOperator projectionSupplier = new ProjectOperator(
				new int[] { 0 });

		DataSourceComponent relationSupplier = new DataSourceComponent(
				"SUPPLIER",
                                dataPath + "supplier" + extension,
				_queryPlan).addOperator(projectionSupplier)
                                           .setHashIndexes(hashSupplier);

		// -------------------------------------------------------------------------------------
		List<Integer> hashNation = Arrays.asList(0);

		ProjectOperator projectionNation = new ProjectOperator(
				new int[] { 1 });

		DataSourceComponent relationNation = new DataSourceComponent("NATION",
				dataPath + "nation" + extension, 
                                _queryPlan).addOperator(projectionNation)
                                           .setHashIndexes(hashNation);

		AggregateOperator agg = new AggregateSumOperator(_doubleConv,
				new ColumnReference(_doubleConv, 0), conf);
		// Empty parameters = Cartesian Product

		ThetaJoinComponent SUPPLIER_NATIONjoin = new ThetaJoinComponent(
				relationSupplier, relationNation, _queryPlan).addOperator(
				new ProjectOperator(new int[] { 0 })).addOperator(agg);

		// -------------------------------------------------------------------------------------

		AggregateOperator overallAgg = new AggregateSumOperator(_doubleConv, new ColumnReference(_doubleConv, 0), conf);

		_queryPlan.setOverallAggregation(overallAgg);

	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}