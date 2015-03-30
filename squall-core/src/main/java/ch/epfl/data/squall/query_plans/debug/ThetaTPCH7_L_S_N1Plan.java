package ch.epfl.data.squall.query_plans.debug;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.squall.conversion.DateConversion;
import ch.epfl.data.squall.conversion.DoubleConversion;
import ch.epfl.data.squall.conversion.IntegerConversion;
import ch.epfl.data.squall.conversion.NumericConversion;
import ch.epfl.data.squall.conversion.StringConversion;
import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.IntegerYearFromDate;
import ch.epfl.data.squall.expressions.Multiplication;
import ch.epfl.data.squall.expressions.Subtraction;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.AggregateCountOperator;
import ch.epfl.data.squall.operators.PrintOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.BetweenPredicate;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.predicates.OrPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.query_plans.theta.ThetaQueryPlansParameters;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;

public class ThetaTPCH7_L_S_N1Plan extends QueryPlan {
	private static Logger LOG = Logger.getLogger(ThetaTPCH7_L_S_N1Plan.class);

	private QueryBuilder _queryBuilder = new QueryBuilder();

	private static final IntegerConversion _ic = new IntegerConversion();

	private static final String _date1Str = "1995-01-01";
	private static final String _date2Str = "1996-12-31";
	private static final String _firstCountryName = "PERU";
	private static final String _secondCountryName = "ETHIOPIA";

	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final TypeConversion<String> _sc = new StringConversion();
	private static final Date _date1 = _dateConv.fromString(_date1Str);
	private static final Date _date2 = _dateConv.fromString(_date2Str);

	public ThetaTPCH7_L_S_N1Plan(String dataPath, String extension, Map conf) {
		// creates materialized relations
		boolean printSelected = MyUtilities.isPrintFilteredLast(conf);
		PrintOperator printL = printSelected ? new PrintOperator("tpch7_l.tbl",
				conf) : null;
		PrintOperator printSN = printSelected ? new PrintOperator(
				"tpch7_sn.tbl", conf) : null;
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

		Component relationLineitem, S_Njoin;
		// first field in projection
		final ValueExpression extractYear = new IntegerYearFromDate(
				new ColumnReference<Date>(_dateConv, 10));
		// second field in projection
		// 1 - discount
		final ValueExpression<Double> substract = new Subtraction(
				new ValueSpecification(_doubleConv, 1.0), new ColumnReference(
						_doubleConv, 6));
		// extendedPrice*(1-discount)
		final ValueExpression<Double> product = new Multiplication(
				new ColumnReference(_doubleConv, 5), substract);
		// third field in projection
		final ColumnReference supplierKey = new ColumnReference(_sc, 2);
		// forth field in projection
		final ColumnReference orderKey = new ColumnReference(_sc, 0);
		final ProjectOperator projectionLineitem = new ProjectOperator(
				extractYear, product, supplierKey, orderKey);
		final ArrayList<Integer> hashLineitem = new ArrayList<Integer>(
				Arrays.asList(2));

		ProjectOperator projectSN = new ProjectOperator(new int[] { 0, 2 });
		final List<Integer> hashSN = new ArrayList<Integer>(Arrays.asList(0));

		if (!isMaterialized) {
			final int Theta_JoinType = ThetaQueryPlansParameters
					.getThetaJoinType(conf);
			// -------------------------------------------------------------------------------------
			final ArrayList<Integer> hashSupplier = new ArrayList<Integer>(
					Arrays.asList(1));

			final ProjectOperator projectionSupplier = new ProjectOperator(
					new int[] { 0, 3 });

			final DataSourceComponent relationSupplier = new DataSourceComponent(
					"SUPPLIER", dataPath + "supplier" + extension)
					.setOutputPartKey(hashSupplier).add(projectionSupplier);
			_queryBuilder.add(relationSupplier);

			// -------------------------------------------------------------------------------------
			final ArrayList<Integer> hashNation1 = new ArrayList<Integer>(
					Arrays.asList(1));

			final SelectOperator selectionNation2 = new SelectOperator(
					new OrPredicate(new ComparisonPredicate(
							new ColumnReference(_sc, 1),
							new ValueSpecification(_sc, _firstCountryName)),
							new ComparisonPredicate(
									new ColumnReference(_sc, 1),
									new ValueSpecification(_sc,
											_secondCountryName))));

			final ProjectOperator projectionNation1 = new ProjectOperator(
					new int[] { 1, 0 });

			final DataSourceComponent relationNation1 = new DataSourceComponent(
					"NATION1", dataPath + "nation" + extension)
					.setOutputPartKey(hashNation1).add(selectionNation2)
					.add(projectionNation1);
			_queryBuilder.add(relationNation1);

			// -------------------------------------------------------------------------------------

			final ColumnReference colS = new ColumnReference(_ic, 1);
			final ColumnReference colN2 = new ColumnReference(_ic, 1);
			final ComparisonPredicate S_N_comp = new ComparisonPredicate(
					ComparisonPredicate.EQUAL_OP, colS, colN2);

			S_Njoin = ThetaJoinComponentFactory
					.createThetaJoinOperator(Theta_JoinType, relationSupplier,
							relationNation1, _queryBuilder).add(printSN)
					.add(projectSN).setJoinPredicate(S_N_comp)
					.setOutputPartKey(hashSN);

			// -------------------------------------------------------------------------------------

			final SelectOperator selectionLineitem = new SelectOperator(
					new BetweenPredicate(new ColumnReference(_dateConv, 10),
							true, new ValueSpecification(_dateConv, _date1),
							true, new ValueSpecification(_dateConv, _date2)));

			relationLineitem = new DataSourceComponent("LINEITEM", dataPath
					+ "lineitem" + extension).setOutputPartKey(hashLineitem)
					.add(selectionLineitem).add(printL).add(projectionLineitem);
			_queryBuilder.add(relationLineitem);
		} else {
			S_Njoin = new DataSourceComponent("SUPPLIER_NATION1", dataPath
					+ "tpch7_sn" + extension).add(projectSN).setOutputPartKey(
					hashSN);
			_queryBuilder.add(S_Njoin);

			relationLineitem = new DataSourceComponent("LINEITEM", dataPath
					+ "tpch7_l" + extension).add(projectionLineitem)
					.setOutputPartKey(hashLineitem);
			_queryBuilder.add(relationLineitem);
		}

		NumericConversion keyType = _ic;
		ComparisonPredicate comparison = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP);
		int firstKeyProject = 2;
		int secondKeyProject = 0;

		if (printSelected) {
			relationLineitem.setPrintOut(false);
			S_Njoin.setPrintOut(false);
		} else if (isOkcanSampling) {
			// TODO there are two projections if we are doing sampling, not such
			// a big deal
			_queryBuilder = MyUtilities.addOkcanSampler(relationLineitem,
					S_Njoin, firstKeyProject, secondKeyProject, _queryBuilder,
					keyType, comparison, conf);
		} else if (isEWHSampling) {
			_queryBuilder = MyUtilities.addEWHSampler(relationLineitem,
					S_Njoin, firstKeyProject, secondKeyProject, _queryBuilder,
					keyType, comparison, conf);
		} else {
			final int Theta_JoinType = ThetaQueryPlansParameters
					.getThetaJoinType(conf);

			final ColumnReference colL = new ColumnReference(keyType,
					firstKeyProject);
			final ColumnReference colS_N = new ColumnReference(keyType,
					secondKeyProject);
			final ComparisonPredicate L_S_N_comp = new ComparisonPredicate(
					ComparisonPredicate.EQUAL_OP, colL, colS_N);

			AggregateCountOperator agg = new AggregateCountOperator(conf);
			Component lastJoiner = ThetaJoinComponentFactory
					.createThetaJoinOperator(Theta_JoinType, relationLineitem,
							S_Njoin, _queryBuilder)
					.add(new ProjectOperator(new int[] { 5, 0, 1, 3 }))
					.setJoinPredicate(L_S_N_comp).add(agg)
					.setContentSensitiveThetaJoinWrapper(keyType);
			// lastJoiner.setPrintOut(false);
		}
	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
