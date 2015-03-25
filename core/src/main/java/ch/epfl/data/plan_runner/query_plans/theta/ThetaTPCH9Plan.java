package ch.epfl.data.plan_runner.query_plans.theta;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.plan_runner.conversion.DateConversion;
import ch.epfl.data.plan_runner.conversion.DoubleConversion;
import ch.epfl.data.plan_runner.conversion.IntegerConversion;
import ch.epfl.data.plan_runner.conversion.NumericConversion;
import ch.epfl.data.plan_runner.conversion.StringConversion;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.expressions.IntegerYearFromDate;
import ch.epfl.data.plan_runner.expressions.Multiplication;
import ch.epfl.data.plan_runner.expressions.Subtraction;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.expressions.ValueSpecification;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.AggregateSumOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.operators.SelectOperator;
import ch.epfl.data.plan_runner.predicates.AndPredicate;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.predicates.LikePredicate;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import ch.epfl.data.plan_runner.query_plans.QueryPlan;

public class ThetaTPCH9Plan extends QueryPlan {
	private static Logger LOG = Logger.getLogger(ThetaTPCH9Plan.class);

	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final StringConversion _sc = new StringConversion();
	private static final IntegerConversion _ic = new IntegerConversion();

	private static final String COLOR = "%green%";

	private QueryBuilder _queryBuilder = new QueryBuilder();

	public ThetaTPCH9Plan(String dataPath, String extension, Map conf) {
		int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
		// -------------------------------------------------------------------------------------
		List<Integer> hashPart = Arrays.asList(0);

		SelectOperator selectionPart = new SelectOperator(
				new LikePredicate(new ColumnReference(_sc, 1),
						new ValueSpecification(_sc, COLOR)));

		ProjectOperator projectionPart = new ProjectOperator(new int[] { 0 });

		DataSourceComponent relationPart = new DataSourceComponent("PART",
				dataPath + "part" + extension).setOutputPartKey(hashPart)
				.add(selectionPart).add(projectionPart);
		_queryBuilder.add(relationPart);

		// -------------------------------------------------------------------------------------
		List<Integer> hashLineitem = Arrays.asList(1);

		ProjectOperator projectionLineitem = new ProjectOperator(new int[] { 0,
				1, 2, 4, 5, 6 });

		DataSourceComponent relationLineitem = new DataSourceComponent(
				"LINEITEM", dataPath + "lineitem" + extension)
				.setOutputPartKey(hashLineitem).add(projectionLineitem);
		_queryBuilder.add(relationLineitem);

		// -------------------------------------------------------------------------------------
		ColumnReference colP = new ColumnReference(_ic, 0);
		ColumnReference colL = new ColumnReference(_ic, 1);
		ComparisonPredicate P_L_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colP, colL);
		Component P_Ljoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationPart,
						relationLineitem, _queryBuilder)
				.setOutputPartKey(Arrays.asList(0, 2))
				.setJoinPredicate(P_L_comp)
				.add(new ProjectOperator(new int[] { 0, 1, 3, 4, 5, 6 }));

		// -------------------------------------------------------------------------------------

		List<Integer> hashPartsupp = Arrays.asList(0, 1);

		ProjectOperator projectionPartsupp = new ProjectOperator(new int[] { 0,
				1, 3 });

		DataSourceComponent relationPartsupp = new DataSourceComponent(
				"PARTSUPP", dataPath + "partsupp" + extension)
				.setOutputPartKey(hashPartsupp).add(projectionPartsupp);
		_queryBuilder.add(relationPartsupp);

		// -------------------------------------------------------------------------------------
		ColumnReference colP_L1 = new ColumnReference(_ic, 0);
		ColumnReference colP_L2 = new ColumnReference(_ic, 2);
		ColumnReference colPS1 = new ColumnReference(_ic, 0);
		ColumnReference colPS2 = new ColumnReference(_ic, 1);
		ComparisonPredicate P_L_PS1_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colP_L1, colPS1);

		ComparisonPredicate P_L_PS2_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colP_L2, colPS2);
		AndPredicate P_L_PS = new AndPredicate(P_L_PS1_comp, P_L_PS2_comp);

		Component P_L_PSjoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, P_Ljoin,
						relationPartsupp, _queryBuilder)
				.setOutputPartKey(Arrays.asList(0))
				.add(new ProjectOperator(new int[] { 1, 2, 3, 4, 5, 8 }))
				.setJoinPredicate(P_L_PS);

		// -------------------------------------------------------------------------------------

		List<Integer> hashOrders = Arrays.asList(0);

		ProjectOperator projectionOrders = new ProjectOperator(
				new ColumnReference(_sc, 0), new IntegerYearFromDate(
						new ColumnReference(_dateConv, 4)));

		DataSourceComponent relationOrders = new DataSourceComponent("ORDERS",
				dataPath + "orders" + extension).setOutputPartKey(hashOrders)
				.add(projectionOrders);
		_queryBuilder.add(relationOrders);

		// -------------------------------------------------------------------------------------
		ColumnReference colP_L_PS = new ColumnReference(_ic, 0);
		ColumnReference colO = new ColumnReference(_ic, 0);
		ComparisonPredicate P_L_PS_O_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colP_L_PS, colO);

		Component P_L_PS_Ojoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, P_L_PSjoin,
						relationOrders, _queryBuilder)
				.add(new ProjectOperator(new int[] { 1, 2, 3, 4, 5, 7 }))
				.setJoinPredicate(P_L_PS_O_comp);

		// -------------------------------------------------------------------------------------

		List<Integer> hashSupplier = Arrays.asList(0);

		ProjectOperator projectionSupplier = new ProjectOperator(new int[] { 0,
				3 });

		DataSourceComponent relationSupplier = new DataSourceComponent(
				"SUPPLIER", dataPath + "supplier" + extension)
				.setOutputPartKey(hashSupplier).add(projectionSupplier);
		_queryBuilder.add(relationSupplier);

		// -------------------------------------------------------------------------------------

		ColumnReference P_L_PS_O = new ColumnReference(_ic, 0);
		ColumnReference colS = new ColumnReference(_ic, 0);
		ComparisonPredicate P_L_PS_O_S_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, P_L_PS_O, colS);

		Component P_L_PS_O_Sjoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, P_L_PS_Ojoin,
						relationSupplier, _queryBuilder)
				.setOutputPartKey(Arrays.asList(5))
				.add(new ProjectOperator(new int[] { 1, 2, 3, 4, 5, 7 }))
				.setJoinPredicate(P_L_PS_O_S_comp);

		// -------------------------------------------------------------------------------------
		List<Integer> hashNation = Arrays.asList(0);

		ProjectOperator projectionNation = new ProjectOperator(
				new int[] { 0, 1 });

		DataSourceComponent relationNation = new DataSourceComponent("NATION",
				dataPath + "nation" + extension).setOutputPartKey(hashNation)
				.add(projectionNation);
		_queryBuilder.add(relationNation);

		// -------------------------------------------------------------------------------------
		// set up aggregation function on the StormComponent(Bolt) where join is
		// performed

		// 1 - discount
		ValueExpression<Double> substract1 = new Subtraction(
				new ValueSpecification(_doubleConv, 1.0), new ColumnReference(
						_doubleConv, 2));
		// extendedPrice*(1-discount)
		ValueExpression<Double> product1 = new Multiplication(
				new ColumnReference(_doubleConv, 1), substract1);

		// ps_supplycost * l_quantity
		ValueExpression<Double> product2 = new Multiplication(
				new ColumnReference(_doubleConv, 3), new ColumnReference(
						_doubleConv, 0));

		// all together
		ValueExpression<Double> substract2 = new Subtraction(product1, product2);

		AggregateOperator agg = new AggregateSumOperator(substract2, conf)
				.setGroupByColumns(Arrays.asList(5, 4));

		ColumnReference P_L_PS_O_S = new ColumnReference(_ic, 5);
		ColumnReference colN = new ColumnReference(_ic, 0);
		ComparisonPredicate P_L_PS_O_S_N_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, P_L_PS_O_S, colN);

		Component P_L_PS_O_S_Njoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, P_L_PS_O_Sjoin,
						relationNation, _queryBuilder)
				.add(new ProjectOperator(new int[] { 0, 1, 2, 3, 4, 7 }))
				.add(agg).setJoinPredicate(P_L_PS_O_S_N_comp);

		// -------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
