package plan_runner.query_plans;

import java.util.Date;
import java.util.Map;

import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.ThetaJoinDynamicComponentAdvisedEpochs;
import plan_runner.components.ThetaJoinStaticComponent;
import plan_runner.conversion.DateConversion;
import plan_runner.conversion.DateIntegerConversion;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateCountOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.AndPredicate;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.utilities.SystemParameters;

public class ThetaLineitemSelfJoin {

	/**
	 * THIS QUERY USES BEREKELYDB STORAGE RATHER THAN IN-MEMORY COMPUTATION
	 */

	/*
	 * Uniform distribution 10G Input = 873.000 + 51.465.000 Output=
	 * 54.206.000.000
	 */

	private final QueryPlan _queryPlan = new QueryPlan();
	private static final String _date1Str = "1993-06-17";
	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final Date _date1 = _dateConv.fromString(_date1Str);
	private static final TypeConversion<String> _stringConv = new StringConversion();

	private static final TypeConversion<Integer> _dateIntConv = new DateIntegerConversion();
	private static final IntegerConversion _ic = new IntegerConversion();

	public ThetaLineitemSelfJoin(String dataPath, String extension, Map conf) {

		final int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
		boolean isBDB = false; // USE BEREKELYDB STORAGE RATHER THAN IN-MEMORY
		// COMPUTATION
		if (SystemParameters.isExisting(conf, "DIP_IS_BDB"))
			isBDB = SystemParameters.getBoolean(conf, "DIP_IS_BDB");

		// Project on shipdate , receiptdate, commitdate, shipInstruct, quantity
		final ProjectOperator projectionLineitem = new ProjectOperator(new int[] { 10, 12, 11, 13,
				4 });

		final ComparisonPredicate comp1 = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
				new ColumnReference(_stringConv, 14), new ValueSpecification(_stringConv, "TRUCK"));
		final ComparisonPredicate comp2 = new ComparisonPredicate(ComparisonPredicate.GREATER_OP,
				new ColumnReference(_ic, 4), new ValueSpecification(_ic, 45));

		final AndPredicate and = new AndPredicate(comp1, comp2);
		final SelectOperator selectionOrders1 = new SelectOperator(and);

		final DataSourceComponent relationLineitem1 = new DataSourceComponent("LINEITEM1", dataPath
				+ "lineitem" + extension, _queryPlan).addOperator(selectionOrders1).addOperator(
				projectionLineitem);

		final SelectOperator selectionOrders2 = new SelectOperator(new ComparisonPredicate(
				ComparisonPredicate.NONEQUAL_OP, new ColumnReference(_stringConv, 14),
				new ValueSpecification(_stringConv, "TRUCK")));
		final DataSourceComponent relationLineitem2 = new DataSourceComponent("LINEITEM2", dataPath
				+ "lineitem" + extension, _queryPlan).addOperator(selectionOrders2).addOperator(
				projectionLineitem);

		new AggregateCountOperator(conf);

		final ColumnReference colLine11 = new ColumnReference(_dateIntConv, 0); // shipdate
		final ColumnReference colLine21 = new ColumnReference(_dateIntConv, 0);

		// B+ TREE or Binary Tree
		/* |col1-col2|<=5 */

		ComparisonPredicate pred5 = null;
		if (!isBDB)
			pred5 = new ComparisonPredicate(ComparisonPredicate.NONGREATER_OP, colLine11,
					colLine21, 1, ComparisonPredicate.BPLUSTREE);
		else
			pred5 = new ComparisonPredicate(ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP, colLine11,
					colLine21, 1);

		Component lastJoiner = null;
		if (Theta_JoinType == 0)
			lastJoiner = new ThetaJoinStaticComponent(relationLineitem1,
					relationLineitem2, _queryPlan).setJoinPredicate(pred5).setBDB(isBDB)
			// .addOperator(agg)
			;
		else if (Theta_JoinType == 1)
			lastJoiner = new ThetaJoinDynamicComponentAdvisedEpochs(relationLineitem1,
					relationLineitem2, _queryPlan).setJoinPredicate(pred5)
			// .addOperator(agg)
			;
		//lastJoiner.setPrintOut(false);

	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}
