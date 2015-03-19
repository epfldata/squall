package ch.epfl.data.plan_runner.query_plans.ewh;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.plan_runner.conversion.DoubleConversion;
import ch.epfl.data.plan_runner.conversion.LongConversion;
import ch.epfl.data.plan_runner.conversion.NumericConversion;
import ch.epfl.data.plan_runner.conversion.StringConversion;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.ewh.components.DummyComponent;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.expressions.ValueSpecification;
import ch.epfl.data.plan_runner.operators.PrintOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.operators.SelectOperator;
import ch.epfl.data.plan_runner.predicates.AndPredicate;
import ch.epfl.data.plan_runner.predicates.BetweenPredicate;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.predicates.Predicate;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import ch.epfl.data.plan_runner.query_plans.theta.ThetaQueryPlansParameters;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.SystemParameters;
import ch.epfl.data.plan_runner.utilities.SystemParameters.HistogramType;

//this is Eocd for high scalability
public class ThetaEWHOrdersScaleJoin {
	private QueryBuilder _queryBuilder = new QueryBuilder();
	private static final TypeConversion<String> _stringConv = new StringConversion();
	private static final LongConversion _lc = new LongConversion();
	private static final DoubleConversion _dc = new DoubleConversion();

	private static ValueSpecification RANGE1_LOWER;
	private static ValueSpecification RANGE1_UPPER;
	private static ValueSpecification RANGE2_LOWER;
	private static ValueSpecification RANGE2_UPPER;

	public ThetaEWHOrdersScaleJoin(String dataPath, String extension, Map conf) {
		// query-specific
		RANGE1_LOWER = new ValueSpecification(_dc, SystemParameters.getDouble(
				conf, "RANGE1_LOWER"));
		RANGE1_UPPER = new ValueSpecification(_dc,
				SystemParameters.getDoubleInfinity(conf, "RANGE1_UPPER"));
		RANGE2_LOWER = RANGE1_LOWER;
		RANGE2_UPPER = RANGE1_UPPER;

		// creates materialized relations
		boolean printSelected = MyUtilities.isPrintFilteredLast(conf);
		String matName1 = "bosc_1";
		String matName2 = "bosc_2";
		PrintOperator print1 = printSelected ? new PrintOperator(matName1
				+ extension, conf) : null;
		PrintOperator print2 = printSelected ? new PrintOperator(matName2
				+ extension, conf) : null;
		// read from materialized relations
		boolean isMaterialized = SystemParameters.isExisting(conf,
				"DIP_MATERIALIZED")
				&& SystemParameters.getBoolean(conf, "DIP_MATERIALIZED");
		boolean isOkcanSampling = SystemParameters.isExisting(conf,
				"DIP_SAMPLING")
				&& SystemParameters.getBoolean(conf, "DIP_SAMPLING");
		boolean isEWHSampling = SystemParameters.isExisting(conf,
				"DIP_EWH_SAMPLING")
				&& SystemParameters.getBoolean(conf, "DIP_EWH_SAMPLING");
		boolean isEWHD2Histogram = SystemParameters.getBooleanIfExist(conf,
				HistogramType.D2_COMB_HIST.genConfEntryName());
		boolean isEWHS1Histogram = SystemParameters.getBooleanIfExist(conf,
				HistogramType.S1_RES_HIST.genConfEntryName());
		boolean isSrcHistogram = isEWHD2Histogram || isEWHS1Histogram;

		Component relationOrders1, relationOrders2;

		// ValueExpression keyField = new ConcatIntString(1, 5); // int
		// conversion
		// ValueExpression keyField = new ConcatIntDouble(1, 3, 10); //long
		// conversion
		ValueExpression keyField = new ColumnReference(_lc, 1);
		ValueExpression orderKey = new ColumnReference(_stringConv, 0);
		ValueExpression orderStatus = new ColumnReference(_stringConv, 2);
		ValueExpression totalPrice = new ColumnReference(_stringConv, 3);
		ValueExpression orderDate = new ColumnReference(_stringConv, 4);
		ProjectOperator projectionLineitem = new ProjectOperator(keyField,
				orderKey, orderStatus, totalPrice, orderDate);

		final List<Integer> hashLineitem = Arrays.asList(0);

		if (!isMaterialized) {
			ComparisonPredicate sel11 = new ComparisonPredicate(
					ComparisonPredicate.EQUAL_OP, new ColumnReference(
							_stringConv, 5), new ValueSpecification(
							_stringConv, "4-NOT SPECIFIED"));
			// ComparisonPredicate sel12 = new
			// ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
			// new ColumnReference(_stringConv, 2), new
			// ValueSpecification(_stringConv, "O"));
			BetweenPredicate sel12 = new BetweenPredicate(new ColumnReference(
					_dc, 3), true, RANGE1_LOWER, false, RANGE1_UPPER);
			Predicate and1 = new AndPredicate(sel11, sel12);
			SelectOperator selectionOrders1 = new SelectOperator(and1);

			relationOrders1 = new DataSourceComponent("ORDERS1", dataPath
					+ "orders" + extension).add(selectionOrders1).add(print1)
					.add(projectionLineitem).setOutputPartKey(hashLineitem);
			_queryBuilder.add(relationOrders1);

			ComparisonPredicate sel21 = new ComparisonPredicate(
					ComparisonPredicate.EQUAL_OP, new ColumnReference(
							_stringConv, 5), new ValueSpecification(
							_stringConv, "1-URGENT"));
			// ComparisonPredicate sel22 = new
			// ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
			// new ColumnReference(_stringConv, 2), new
			// ValueSpecification(_stringConv, "O"));
			BetweenPredicate sel22 = new BetweenPredicate(new ColumnReference(
					_dc, 3), true, RANGE2_LOWER, false, RANGE2_UPPER);
			Predicate and2 = new AndPredicate(sel21, sel22);
			SelectOperator selectionOrders2 = new SelectOperator(and2);

			relationOrders2 = new DataSourceComponent("ORDERS2", dataPath
					+ "orders" + extension).add(selectionOrders2).add(print2)
					.add(projectionLineitem).setOutputPartKey(hashLineitem);
			_queryBuilder.add(relationOrders2);
		} else {
			relationOrders1 = new DataSourceComponent("ORDERS1", dataPath
					+ matName1 + extension).add(projectionLineitem)
					.setOutputPartKey(hashLineitem);
			_queryBuilder.add(relationOrders1);

			relationOrders2 = new DataSourceComponent("LINEITEM2", dataPath
					+ matName2 + extension).add(projectionLineitem)
					.setOutputPartKey(hashLineitem);
			_queryBuilder.add(relationOrders2);
		}

		NumericConversion keyType = _lc;
		ComparisonPredicate comparison = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP);
		int firstKeyProject = 0;
		int secondKeyProject = 0;

		if (printSelected) {
			relationOrders1.setPrintOut(false);
			relationOrders2.setPrintOut(false);
		} else if (isSrcHistogram) {
			_queryBuilder = MyUtilities.addSrcHistogram(relationOrders1,
					firstKeyProject, relationOrders2, secondKeyProject,
					keyType, comparison, isEWHD2Histogram, isEWHS1Histogram,
					conf);
		} else if (isOkcanSampling) {
			_queryBuilder = MyUtilities.addOkcanSampler(relationOrders1,
					relationOrders2, firstKeyProject, secondKeyProject,
					_queryBuilder, keyType, comparison, conf);
		} else if (isEWHSampling) {
			_queryBuilder = MyUtilities.addEWHSampler(relationOrders1,
					relationOrders2, firstKeyProject, secondKeyProject,
					_queryBuilder, keyType, comparison, conf);
		} else {
			final int Theta_JoinType = ThetaQueryPlansParameters
					.getThetaJoinType(conf);
			final ColumnReference colO1 = new ColumnReference(keyType,
					firstKeyProject);
			final ColumnReference colO2 = new ColumnReference(keyType,
					secondKeyProject);
			// Addition expr2 = new Addition(colO2, new ValueSpecification(_ic,
			// keyOffset));
			final ComparisonPredicate O1_O2_comp = new ComparisonPredicate(
					ComparisonPredicate.EQUAL_OP, colO1, colO2);

			// AggregateCountOperator agg = new AggregateCountOperator(conf);
			Component lastJoiner = ThetaJoinComponentFactory
					.createThetaJoinOperator(Theta_JoinType, relationOrders1,
							relationOrders2, _queryBuilder)
					.setJoinPredicate(O1_O2_comp)
					.setContentSensitiveThetaJoinWrapper(keyType);
			// .addOperator(agg)
			// lastJoiner.setPrintOut(false);

			DummyComponent dummy = new DummyComponent(lastJoiner, "DUMMY");
			_queryBuilder.add(dummy);
		}

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}