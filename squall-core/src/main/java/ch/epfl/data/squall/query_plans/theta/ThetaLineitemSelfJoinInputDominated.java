package ch.epfl.data.squall.query_plans.theta;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.squall.conversion.DateConversion;
import ch.epfl.data.squall.conversion.DateIntegerConversion;
import ch.epfl.data.squall.conversion.DoubleConversion;
import ch.epfl.data.squall.conversion.IntegerConversion;
import ch.epfl.data.squall.conversion.StringConversion;
import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.AggregateCountOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.AndPredicate;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.predicates.OrPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;

//BNCI
public class ThetaLineitemSelfJoinInputDominated {

	/*
	 * For 0.01G With comp2 = 34, seleOrders2 decommented Input = 2844 + 51465 =
	 * 54309 Output = 26791
	 * 
	 * Also old: Input = 1214 + 45165 Output = 10880
	 * 
	 * New: Input = 873 + 30057, Output = 5194
	 * 
	 * New new: Input: 341 + 15010 Output = 1073
	 */

	private QueryBuilder _queryBuilder = new QueryBuilder();
	private static final String _date1Str = "1993-06-17";
	private static final TypeConversion<Date> _dateConv = new DateConversion();
	// private static final NumericConversion<Double> _doubleConv = new
	// DoubleConversion();
	private static final Date _date1 = _dateConv.fromString(_date1Str);
	private static final TypeConversion<String> _stringConv = new StringConversion();

	private static final TypeConversion<Integer> _dateIntConv = new DateIntegerConversion();
	private static final IntegerConversion _ic = new IntegerConversion();
	private static final DoubleConversion _dblConv = new DoubleConversion();

	public ThetaLineitemSelfJoinInputDominated(String dataPath,
			String extension, Map conf) {

		int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
		boolean isBDB = MyUtilities.isBDB(conf);

		int quantityBound = 48;
		if (!SystemParameters.getBoolean(conf, "DIP_DISTRIBUTED")) {
			quantityBound = 46;
		}

		// Project on shipdate, receiptdate, commitdate, shipInstruct, quantity,
		// orderkey
		ProjectOperator projectionLineitem = new ProjectOperator(new int[] {
				10, 12, 11, 13, 4, 0 });
		final List<Integer> hashLineitem = Arrays.asList(5);

		ComparisonPredicate comp1 = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, new ColumnReference(_stringConv,
						14), new ValueSpecification(_stringConv, "TRUCK"));
		ComparisonPredicate comp2 = new ComparisonPredicate(
				ComparisonPredicate.GREATER_OP, new ColumnReference(_ic, 4),
				new ValueSpecification(_ic, quantityBound));

		AndPredicate and = new AndPredicate(comp1, comp2);
		SelectOperator selectionOrders1 = new SelectOperator(and);

		DataSourceComponent relationLineitem1 = new DataSourceComponent(
				"LINEITEM1", dataPath + "lineitem" + extension)
				.add(selectionOrders1).add(projectionLineitem)
				.setOutputPartKey(hashLineitem);
		_queryBuilder.add(relationLineitem1);

		// SelectOperator selectionOrders2 = new SelectOperator(new
		// ComparisonPredicate(ComparisonPredicate.NONEQUAL_OP, new
		// ColumnReference(_stringConv, 14), new ValueSpecification(_stringConv,
		// "TRUCK")));
		ComparisonPredicate cond1 = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, new ColumnReference(_stringConv,
						13), new ValueSpecification(_stringConv, "NONE"));
		ComparisonPredicate cond2 = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, new ColumnReference(_stringConv,
						13), new ValueSpecification(_stringConv, "COLLECT COD"));
		OrPredicate and2 = new OrPredicate(cond1, cond2);
		// SelectOperator selectionOrders2 = new SelectOperator(and2);
		SelectOperator selectionOrders2 = new SelectOperator(cond1);
		DataSourceComponent relationLineitem2 = new DataSourceComponent(
				"LINEITEM2", dataPath + "lineitem" + extension)
				.add(selectionOrders2).add(projectionLineitem)
				.setOutputPartKey(hashLineitem);
		_queryBuilder.add(relationLineitem2);

		// ColumnReference colLine11 = new ColumnReference(_dateIntConv, 0);
		// //shipdate
		// ColumnReference colLine12 = new ColumnReference(_dateConv, 1);
		// //receiptdate
		ColumnReference colLine11 = new ColumnReference(_ic, 5); // orderkey

		// ColumnReference colLine21 = new ColumnReference(_dateIntConv, 0);
		// ColumnReference colLine22 = new ColumnReference(_dateConv, 1);
		ColumnReference colLine21 = new ColumnReference(_ic, 5); // orderkey

		// INTERVAL
		// IntervalPredicate pred3 = new IntervalPredicate(colLine11, colLine12,
		// colLine22, colLine22);
		// DateSum add2= new DateSum(colLine22, Calendar.DAY_OF_MONTH, 2);
		// IntervalPredicate pred4 = new IntervalPredicate(colLine12, colLine12,
		// colLine22, add2);

		// B+ TREE or Binary Tree
		/*
		 * |col1-col2|<=5
		 */
		ComparisonPredicate pred5 = null;
		if (!isBDB) {
			pred5 = new ComparisonPredicate(ComparisonPredicate.NONGREATER_OP,
					colLine11, colLine21, 1, ComparisonPredicate.BPLUSTREE);
			// ComparisonPredicate pred5 = new
			// ComparisonPredicate(ComparisonPredicate.LESS_OP,colLine11,
			// colLine21, 1, ComparisonPredicate.BPLUSTREE);
			// ComparisonPredicate pred5 = new
			// ComparisonPredicate(ComparisonPredicate.NONGREATER_OP,colLine11,
			// colLine21, 1, ComparisonPredicate.BINARYTREE);
		} else {
			pred5 = new ComparisonPredicate(
					ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP, colLine11,
					colLine21, 1);
		}

		AggregateCountOperator agg = new AggregateCountOperator(conf);
		Component LINEITEMS_LINEITEMSjoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationLineitem1,
						relationLineitem2, _queryBuilder)
				.setJoinPredicate(pred5)
				.setContentSensitiveThetaJoinWrapper(_ic).add(agg);

		// LINEITEMS_LINEITEMSjoin.setPrintOut(false);

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
