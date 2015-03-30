package ch.epfl.data.squall.query_plans.debug;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.EquiJoinComponent;
import ch.epfl.data.squall.conversion.DateConversion;
import ch.epfl.data.squall.conversion.DoubleConversion;
import ch.epfl.data.squall.conversion.IntegerConversion;
import ch.epfl.data.squall.conversion.NumericConversion;
import ch.epfl.data.squall.conversion.StringConversion;
import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;

public class TPCH8_9_P_LPlan {
	private static Logger LOG = Logger.getLogger(TPCH8_9_P_LPlan.class);

	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final StringConversion _sc = new StringConversion();

	private static final String COLOR = "%green%";

	private QueryBuilder _queryBuilder = new QueryBuilder();

	private static final IntegerConversion _ic = new IntegerConversion();

	public TPCH8_9_P_LPlan(String dataPath, String extension, Map conf) {
		// -------------------------------------------------------------------------------------
		List<Integer> hashPart = Arrays.asList(0);

		ProjectOperator projectionPart = new ProjectOperator(new int[] { 0 });

		DataSourceComponent relationPart = new DataSourceComponent("PART",
				dataPath + "part" + extension).setOutputPartKey(hashPart)
		// .addOperator(selectionPart)
				.add(projectionPart);
		_queryBuilder.add(relationPart);

		// -------------------------------------------------------------------------------------
		List<Integer> hashLineitem = Arrays.asList(1);

		ProjectOperator projectionLineitem = new ProjectOperator(new int[] { 0,
				1, 2, 4, 5, 6 });

		DataSourceComponent relationLineitem = new DataSourceComponent(
				"LINEITEM", dataPath + "lineitem" + extension)
				.setOutputPartKey(hashLineitem).add(projectionLineitem);
		_queryBuilder.add(relationLineitem);

		// AggregateCountOperator agg= new AggregateCountOperator(conf);

		// -------------------------------------------------------------------------------------
		/*
		 * EquiJoinComponent P_Ljoin = new EquiJoinComponent( relationPart,
		 * relationLineitem, _queryPlan).setHashIndexes(Arrays.asList(0, 2)) //
		 * .addOperator(agg) ; P_Ljoin.setPrintOut(false);
		 */

		ColumnReference colP = new ColumnReference(_ic, 0);
		ColumnReference colL = new ColumnReference(_ic, 1);
		ComparisonPredicate P_L_pred = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colP, colL);

		EquiJoinComponent P_Ljoin = new EquiJoinComponent(relationPart,
				relationLineitem).setOutputPartKey(Arrays.asList(0, 2))
				.setJoinPredicate(P_L_pred)
				.add(new ProjectOperator(new int[] { 0, 1, 3, 4, 5, 6 }));
		_queryBuilder.add(P_Ljoin);
		P_Ljoin.setPrintOut(false);

		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
