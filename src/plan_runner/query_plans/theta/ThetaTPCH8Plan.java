package plan_runner.query_plans.theta;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.theta.ThetaJoinComponentFactory;
import plan_runner.components.theta.ThetaJoinDynamicComponentAdvisedEpochs;
import plan_runner.components.theta.ThetaJoinStaticComponent;
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
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.BetweenPredicate;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.query_plans.QueryBuilder;
import plan_runner.query_plans.theta.ThetaQueryPlansParameters;

public class ThetaTPCH8Plan {
	private static Logger LOG = Logger.getLogger(ThetaTPCH8Plan.class);

	private QueryBuilder _queryBuilder = new QueryBuilder();

	// the field nation is not used, since we cannot provide final result if having more final components
	private static final String _nation = "BRAZIL";
	private static final String _region = "AMERICA";
	private static final String _type = "ECONOMY ANODIZED STEEL";
	private static final String _date1Str = "1995-01-01";
	private static final String _date2Str = "1996-12-31";

	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final TypeConversion<String> _sc = new StringConversion();

	private static final Date _date1 = _dateConv.fromString(_date1Str);
	private static final Date _date2 = _dateConv.fromString(_date2Str);

	private static final IntegerConversion _ic = new IntegerConversion();

	public ThetaTPCH8Plan(String dataPath, String extension, Map conf) {

		int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);

		//-------------------------------------------------------------------------------------
		List<Integer> hashRegion = Arrays.asList(0);

		SelectOperator selectionRegion = new SelectOperator(new ComparisonPredicate(
				new ColumnReference(_sc, 1), new ValueSpecification(_sc, _region)));

		ProjectOperator projectionRegion = new ProjectOperator(new int[] { 0 });

		DataSourceComponent relationRegion = new DataSourceComponent("REGION", dataPath + "region"
				+ extension).setOutputPartKey(hashRegion).add(selectionRegion)
				.add(projectionRegion);
		_queryBuilder.add(relationRegion);

		//-------------------------------------------------------------------------------------
		List<Integer> hashNation1 = Arrays.asList(1);

		ProjectOperator projectionNation1 = new ProjectOperator(new int[] { 0, 2 });

		DataSourceComponent relationNation1 = new DataSourceComponent("NATION1", dataPath
				+ "nation" + extension).setOutputPartKey(hashNation1).add(
				projectionNation1);
		_queryBuilder.add(relationNation1);

		//-------------------------------------------------------------------------------------
		ColumnReference colR = new ColumnReference(_ic, 0);
		ColumnReference colN = new ColumnReference(_ic, 1);
		ComparisonPredicate R_N_comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, colR,
				colN);

		Component R_Njoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationRegion, relationNation1,
						_queryBuilder).add(new ProjectOperator(new int[] { 1 }))
				.setOutputPartKey(Arrays.asList(0)).setJoinPredicate(R_N_comp);

		//-------------------------------------------------------------------------------------
		List<Integer> hashCustomer = Arrays.asList(0);

		ProjectOperator projectionCustomer = new ProjectOperator(new int[] { 3, 0 });

		DataSourceComponent relationCustomer = new DataSourceComponent("CUSTOMER", dataPath
				+ "customer" + extension).setOutputPartKey(hashCustomer).add(
				projectionCustomer);
		_queryBuilder.add(relationCustomer);

		//-------------------------------------------------------------------------------------

		ColumnReference colR_N = new ColumnReference(_ic, 0);
		ColumnReference colC = new ColumnReference(_ic, 0);
		ComparisonPredicate R_N_C_comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
				colR_N, colC);
		Component R_N_Cjoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, R_Njoin, relationCustomer, _queryBuilder)
				.add(new ProjectOperator(new int[] { 2 })).setOutputPartKey(Arrays.asList(0))
				.setJoinPredicate(R_N_C_comp);

		//-------------------------------------------------------------------------------------
		List<Integer> hashSupplier = Arrays.asList(1);

		ProjectOperator projectionSupplier = new ProjectOperator(new int[] { 0, 3 });

		DataSourceComponent relationSupplier = new DataSourceComponent("SUPPLIER", dataPath
				+ "supplier" + extension).setOutputPartKey(hashSupplier).add(
				projectionSupplier);
		_queryBuilder.add(relationSupplier);

		//-------------------------------------------------------------------------------------
		List<Integer> hashNation2 = Arrays.asList(0);

		ProjectOperator projectionNation2 = new ProjectOperator(new int[] { 0, 1 });

		DataSourceComponent relationNation2 = new DataSourceComponent("NATION2", dataPath
				+ "nation" + extension).setOutputPartKey(hashNation2).add(
				projectionNation2);
		_queryBuilder.add(relationNation2);

		//-------------------------------------------------------------------------------------
		ColumnReference colS = new ColumnReference(_ic, 1);
		ColumnReference colN2 = new ColumnReference(_ic, 0);
		ComparisonPredicate S_N2_comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, colS,
				colN2);

		Component S_Njoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationSupplier, relationNation2,
						_queryBuilder).add(new ProjectOperator(new int[] { 0, 3 }))
				.setOutputPartKey(Arrays.asList(0)).setJoinPredicate(S_N2_comp);

		//-------------------------------------------------------------------------------------
		List<Integer> hashPart = Arrays.asList(0);

		SelectOperator selectionPart = new SelectOperator(new ComparisonPredicate(
				new ColumnReference(_sc, 4), new ValueSpecification(_sc, _type)));

		ProjectOperator projectionPart = new ProjectOperator(new int[] { 0 });

		DataSourceComponent relationPart = new DataSourceComponent("PART", dataPath + "part"
				+ extension).setOutputPartKey(hashPart).add(selectionPart)
				.add(projectionPart);
		_queryBuilder.add(relationPart);

		//-------------------------------------------------------------------------------------
		List<Integer> hashLineitem = Arrays.asList(1);

		//first field in projection
		ColumnReference orderKey = new ColumnReference(_sc, 0);
		//second field in projection
		ColumnReference partKey = new ColumnReference(_sc, 1);
		//third field in projection
		ColumnReference suppKey = new ColumnReference(_sc, 2);
		//forth field in projection
		ValueExpression<Double> substract = new Subtraction(
				new ValueSpecification(_doubleConv, 1.0), new ColumnReference(_doubleConv, 6));
		//extendedPrice*(1-discount)
		ValueExpression<Double> product = new Multiplication(new ColumnReference(_doubleConv, 5),
				substract);
		ProjectOperator projectionLineitem = new ProjectOperator(orderKey, partKey, suppKey,
				product);

		DataSourceComponent relationLineitem = new DataSourceComponent("LINEITEM", dataPath
				+ "lineitem" + extension).setOutputPartKey(hashLineitem).add(
				projectionLineitem);
		_queryBuilder.add(relationLineitem);

		//-------------------------------------------------------------------------------------
		ColumnReference colP = new ColumnReference(_ic, 0);
		ColumnReference colL = new ColumnReference(_ic, 1);
		ComparisonPredicate P_L_comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, colP,
				colL);

		Component P_Ljoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationPart, relationLineitem, _queryBuilder)
				.add(new ProjectOperator(new int[] { 1, 3, 4 }))
				.setOutputPartKey(Arrays.asList(0)).setJoinPredicate(P_L_comp)
		//	                           .addOperator(agg)
		;

		//-------------------------------------------------------------------------------------
		List<Integer> hashOrders = Arrays.asList(0);

		SelectOperator selectionOrders = new SelectOperator(new BetweenPredicate(
				new ColumnReference(_dateConv, 4), true, new ValueSpecification(_dateConv, _date1),
				true, new ValueSpecification(_dateConv, _date2)));

		//first field in projection
		ValueExpression OrdersOrderKey = new ColumnReference(_sc, 0);
		//second field in projection
		ValueExpression OrdersCustKey = new ColumnReference(_sc, 1);
		//third field in projection
		ValueExpression OrdersExtractYear = new IntegerYearFromDate(new ColumnReference<Date>(
				_dateConv, 4));
		ProjectOperator projectionOrders = new ProjectOperator(OrdersOrderKey, OrdersCustKey,
				OrdersExtractYear);

		DataSourceComponent relationOrders = new DataSourceComponent("ORDERS", dataPath + "orders"
				+ extension).setOutputPartKey(hashOrders).add(selectionOrders)
				.add(projectionOrders);
		_queryBuilder.add(relationOrders);

		//-------------------------------------------------------------------------------------
		ColumnReference colP_L = new ColumnReference(_ic, 0);
		ColumnReference colO = new ColumnReference(_ic, 0);
		ComparisonPredicate P_L_O_comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
				colP_L, colO);

		Component P_L_Ojoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, P_Ljoin, relationOrders, _queryBuilder)
				.add(new ProjectOperator(new int[] { 1, 2, 4, 5 }))
				.setOutputPartKey(Arrays.asList(0)).setJoinPredicate(P_L_O_comp);

		//-------------------------------------------------------------------------------------
		ColumnReference colS_N = new ColumnReference(_ic, 0);
		ColumnReference colP_L_O = new ColumnReference(_ic, 0);
		ComparisonPredicate S_N_P_L_O_comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
				colS_N, colP_L_O);

		Component S_N_P_L_Ojoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, S_Njoin, P_L_Ojoin, _queryBuilder)
				.add(new ProjectOperator(new int[] { 1, 3, 4, 5 }))
				.setOutputPartKey(Arrays.asList(2)).setJoinPredicate(S_N_P_L_O_comp);

		//-------------------------------------------------------------------------------------
		AggregateOperator agg = new AggregateSumOperator(new ColumnReference(_doubleConv, 2), conf)
				.setGroupByColumns(Arrays.asList(1, 4));

		ColumnReference colR_N_C = new ColumnReference(_ic, 0);
		ColumnReference colS_N_P_L_O = new ColumnReference(_ic, 2);
		ComparisonPredicate R_N_C_S_N_P_L_O_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colR_N_C, colS_N_P_L_O);

		Component R_N_C_S_N_P_L_Ojoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, R_N_Cjoin, S_N_P_L_Ojoin, _queryBuilder)
				.add(agg).setJoinPredicate(R_N_C_S_N_P_L_O_comp);

		//-------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}