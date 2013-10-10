package plan_runner.query_plans;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.ThetaJoinDynamicComponentAdvisedEpochs;
import plan_runner.components.ThetaJoinStaticComponent;
import plan_runner.conversion.DateConversion;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.IntegerYearFromDate;
import plan_runner.expressions.Multiplication;
import plan_runner.expressions.Subtraction;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateCountOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.BetweenPredicate;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.predicates.OrPredicate;

public class ThetaTPCH7_L_S_N1Plan {
	private static Logger LOG = Logger.getLogger(ThetaTPCH7_L_S_N1Plan.class);

	private final QueryPlan _queryPlan = new QueryPlan();

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
		final int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
		// -------------------------------------------------------------------------------------
		final ArrayList<Integer> hashSupplier = new ArrayList<Integer>(Arrays.asList(1));

		final ProjectOperator projectionSupplier = new ProjectOperator(new int[] { 0, 3 });

		final DataSourceComponent relationSupplier = new DataSourceComponent("SUPPLIER", dataPath
				+ "supplier" + extension, _queryPlan).setHashIndexes(hashSupplier).addOperator(
				projectionSupplier);

		// -------------------------------------------------------------------------------------
		final ArrayList<Integer> hashNation1 = new ArrayList<Integer>(Arrays.asList(1));

		final SelectOperator selectionNation2 = new SelectOperator(new OrPredicate(
				new ComparisonPredicate(new ColumnReference(_sc, 1), new ValueSpecification(_sc,
						_firstCountryName)), new ComparisonPredicate(new ColumnReference(_sc, 1),
						new ValueSpecification(_sc, _secondCountryName))));

		final ProjectOperator projectionNation1 = new ProjectOperator(new int[] { 1, 0 });

		final DataSourceComponent relationNation1 = new DataSourceComponent("NATION1", dataPath
				+ "nation" + extension, _queryPlan).setHashIndexes(hashNation1)
				.addOperator(selectionNation2).addOperator(projectionNation1);

		// -------------------------------------------------------------------------------------

		final ColumnReference colS = new ColumnReference(_ic, 1);
		final ColumnReference colN2 = new ColumnReference(_ic, 1);
		final ComparisonPredicate S_N_comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
				colS, colN2);

		Component S_Njoin = null;

		if (Theta_JoinType == 0)
			S_Njoin = new ThetaJoinStaticComponent(relationSupplier, relationNation1, _queryPlan)
					.addOperator(new ProjectOperator(new int[] { 0, 2 }))
					.setJoinPredicate(S_N_comp);
		else if (Theta_JoinType == 1)
			S_Njoin = new ThetaJoinDynamicComponentAdvisedEpochs(relationSupplier, relationNation1,
					_queryPlan).addOperator(new ProjectOperator(new int[] { 0, 2 }))
					.setJoinPredicate(S_N_comp);

		// -------------------------------------------------------------------------------------
		final ArrayList<Integer> hashLineitem = new ArrayList<Integer>(Arrays.asList(2));

		final SelectOperator selectionLineitem = new SelectOperator(new BetweenPredicate(
				new ColumnReference(_dateConv, 10), true,
				new ValueSpecification(_dateConv, _date1), true, new ValueSpecification(_dateConv,
						_date2)));

		// first field in projection
		final ValueExpression extractYear = new IntegerYearFromDate(new ColumnReference<Date>(
				_dateConv, 10));
		// second field in projection
		// 1 - discount
		final ValueExpression<Double> substract = new Subtraction(new ValueSpecification(
				_doubleConv, 1.0), new ColumnReference(_doubleConv, 6));
		// extendedPrice*(1-discount)
		final ValueExpression<Double> product = new Multiplication(new ColumnReference(_doubleConv,
				5), substract);
		// third field in projection
		final ColumnReference supplierKey = new ColumnReference(_sc, 2);
		// forth field in projection
		final ColumnReference orderKey = new ColumnReference(_sc, 0);
		final ProjectOperator projectionLineitem = new ProjectOperator(extractYear, product,
				supplierKey, orderKey);

		final DataSourceComponent relationLineitem = new DataSourceComponent("LINEITEM", dataPath
				+ "lineitem" + extension, _queryPlan).setHashIndexes(hashLineitem)
				.addOperator(selectionLineitem).addOperator(projectionLineitem);

		// -------------------------------------------------------------------------------------

		final ColumnReference colL = new ColumnReference(_ic, 2);
		final ColumnReference colS_N = new ColumnReference(_ic, 0);
		final ComparisonPredicate L_S_N_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colL, colS_N);

		final AggregateCountOperator agg = new AggregateCountOperator(conf);

		Component lastJoiner = null;
		if (Theta_JoinType == 0)
			lastJoiner = new ThetaJoinStaticComponent(relationLineitem, S_Njoin, _queryPlan)
					.addOperator(new ProjectOperator(new int[] { 5, 0, 1, 3 }))
					.setJoinPredicate(L_S_N_comp).addOperator(agg);
		else if (Theta_JoinType == 1)
			lastJoiner = new ThetaJoinDynamicComponentAdvisedEpochs(relationLineitem, S_Njoin, _queryPlan)
					.addOperator(new ProjectOperator(new int[] { 5, 0, 1, 3 }))
					.setJoinPredicate(L_S_N_comp).addOperator(agg);
		lastJoiner.setPrintOut(false);
	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}