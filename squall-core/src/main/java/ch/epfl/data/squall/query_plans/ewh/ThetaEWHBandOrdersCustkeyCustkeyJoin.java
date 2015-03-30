package ch.epfl.data.squall.query_plans.ewh;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.squall.conversion.DateIntegerConversion;
import ch.epfl.data.squall.conversion.IntegerConversion;
import ch.epfl.data.squall.conversion.NumericConversion;
import ch.epfl.data.squall.conversion.StringConversion;
import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.ewh.components.DummyComponent;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.PrintOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.AndPredicate;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.theta.ThetaQueryPlansParameters;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;

public class ThetaEWHBandOrdersCustkeyCustkeyJoin {
	private QueryBuilder _queryBuilder = new QueryBuilder();
	private static final TypeConversion<String> _stringConv = new StringConversion();
	private static final IntegerConversion _ic = new IntegerConversion();
	private DateIntegerConversion _dic = new DateIntegerConversion();

	public ThetaEWHBandOrdersCustkeyCustkeyJoin(String dataPath,
			String extension, Map conf) {
		// ORDERS * ORDERS on orderkey equi
		// I = 2 * 15M = 30M; O =
		// Variability is [0, 10] * skew
		// baseline + z1 + select date (4 and < 19960101, 1): (1.7m, 3m, 5m): no
		// output skew
		// 1Bucket 107s, MBucket 43s, EWH 42s

		// creates materialized relations
		boolean printSelected = MyUtilities.isPrintFilteredLast(conf);
		String matName1 = "bbosc_1";
		String matName2 = "bbosc_2";
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

		Component relationOrders1, relationOrders2;
		// Project on shipdate , receiptdate, commitdate, shipInstruct, quantity
		ProjectOperator projectionLineitem = new ProjectOperator(new int[] { 0,
				2, 3, 4, 5, 1 });
		final List<Integer> hashLineitem = Arrays.asList(5);

		if (!isMaterialized) {
			// ORDERDATE NO: startdate - enddate - 151, but for z4 mostly are
			// 1996-01-02
			// STARTDATE = 1992-01-01 CURRENTDATE = 1995-06-17 ENDDATE =
			// 1998-12-31
			Integer dateBoundary = 19960101;
			ComparisonPredicate sel11 = new ComparisonPredicate(
					ComparisonPredicate.EQUAL_OP, new ColumnReference(
							_stringConv, 5), new ValueSpecification(
							_stringConv, "4-NOT SPECIFIED"));
			ComparisonPredicate sel12 = new ComparisonPredicate(
					ComparisonPredicate.LESS_OP, new ColumnReference(_dic, 4),
					new ValueSpecification(_dic, dateBoundary));
			AndPredicate andOrders1 = new AndPredicate(sel11, sel12);
			SelectOperator selectionOrders1 = new SelectOperator(andOrders1);

			relationOrders1 = new DataSourceComponent("ORDERS1", dataPath
					+ "orders" + extension).add(selectionOrders1).add(print1)
					.add(projectionLineitem).setOutputPartKey(hashLineitem);
			_queryBuilder.add(relationOrders1);

			ComparisonPredicate sel21 = new ComparisonPredicate(
					ComparisonPredicate.EQUAL_OP, new ColumnReference(
							_stringConv, 5), new ValueSpecification(
							_stringConv, "1-URGENT"));
			SelectOperator selectionOrders2 = new SelectOperator(sel21);

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

		NumericConversion keyType = _ic;
		int comparisonValue = 1;
		ComparisonPredicate comparison = new ComparisonPredicate(
				ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP, comparisonValue,
				keyType);
		int firstKeyProject = 5;
		int secondKeyProject = 5;

		if (printSelected) {
			relationOrders1.setPrintOut(false);
			relationOrders2.setPrintOut(false);
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

			ComparisonPredicate pred5 = new ComparisonPredicate(
					ComparisonPredicate.NONGREATER_OP, colO1, colO2,
					comparisonValue, ComparisonPredicate.BPLUSTREE);

			// AggregateCountOperator agg = new AggregateCountOperator(conf);
			Component lastJoiner = ThetaJoinComponentFactory
					.createThetaJoinOperator(Theta_JoinType, relationOrders1,
							relationOrders2, _queryBuilder)
					.setJoinPredicate(pred5)
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