package plan_runner.query_plans;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.ThetaJoinStaticComponent;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.ProjectOperator;


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

		AggregateOperator agg = new AggregateSumOperator(
				new ColumnReference(_doubleConv, 0), conf);
		// Empty parameters = Cartesian Product

		ThetaJoinStaticComponent SUPPLIER_NATIONjoin = new ThetaJoinStaticComponent(
				relationSupplier, relationNation, _queryPlan).addOperator(
				new ProjectOperator(new int[] { 0 })).addOperator(agg);

		// -------------------------------------------------------------------------------------

	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}