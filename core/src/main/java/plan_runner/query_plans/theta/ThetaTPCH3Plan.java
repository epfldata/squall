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
import plan_runner.expressions.Multiplication;
import plan_runner.expressions.Subtraction;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.query_plans.QueryBuilder;

public class ThetaTPCH3Plan {
	private static Logger LOG = Logger.getLogger(ThetaTPCH3Plan.class);

	private static final IntegerConversion _ic = new IntegerConversion();

	private static final String _customerMktSegment = "BUILDING";
	private static final String _dateStr = "1995-03-15";

	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final TypeConversion<String> _sc = new StringConversion();
	private static final Date _date = _dateConv.fromString(_dateStr);

	private QueryBuilder _queryBuilder = new QueryBuilder();

	public ThetaTPCH3Plan(String dataPath, String extension, Map conf) {

		int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
		//-------------------------------------------------------------------------------------
		List<Integer> hashCustomer = Arrays.asList(0);

		SelectOperator selectionCustomer = new SelectOperator(new ComparisonPredicate(
				new ColumnReference(_sc, 6), new ValueSpecification(_sc, _customerMktSegment)));
		
		ProjectOperator projectionCustomer = new ProjectOperator(new int[] { 0 });

		DataSourceComponent relationCustomer = new DataSourceComponent("CUSTOMER", dataPath
				+ "customer" + extension).setOutputPartKey(hashCustomer)
				.add(selectionCustomer).add(projectionCustomer);
		_queryBuilder.add(relationCustomer);

		//-------------------------------------------------------------------------------------
		List<Integer> hashOrders = Arrays.asList(1);

		SelectOperator selectionOrders = new SelectOperator(new ComparisonPredicate(
				ComparisonPredicate.LESS_OP, new ColumnReference(_dateConv, 4),
				new ValueSpecification(_dateConv, _date)));

		ProjectOperator projectionOrders = new ProjectOperator(new int[] { 0, 1, 4, 7 });

		DataSourceComponent relationOrders = new DataSourceComponent("ORDERS", dataPath + "orders"
				+ extension).setOutputPartKey(hashOrders).add(selectionOrders)
				.add(projectionOrders);
		_queryBuilder.add(relationOrders);

		ColumnReference colC = new ColumnReference(_ic, 0);
		ColumnReference colO = new ColumnReference(_ic, 1);
		ComparisonPredicate C_O_comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, colC,
				colO);

		//-------------------------------------------------------------------------------------
		Component C_Ojoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, relationCustomer, relationOrders,
						_queryBuilder).add(new ProjectOperator(new int[] { 1, 2, 3 }))
				.setOutputPartKey(Arrays.asList(0)).setJoinPredicate(C_O_comp);

		//-------------------------------------------------------------------------------------
		List<Integer> hashLineitem = Arrays.asList(0);

		SelectOperator selectionLineitem = new SelectOperator(new ComparisonPredicate(
				ComparisonPredicate.GREATER_OP, new ColumnReference(_dateConv, 10),
				new ValueSpecification(_dateConv, _date)));

		ProjectOperator projectionLineitem = new ProjectOperator(new int[] { 0, 5, 6 });

		DataSourceComponent relationLineitem = new DataSourceComponent("LINEITEM", dataPath
				+ "lineitem" + extension).setOutputPartKey(hashLineitem)
				.add(selectionLineitem).add(projectionLineitem);
		_queryBuilder.add(relationLineitem);
		
		//-------------------------------------------------------------------------------------
		// set up aggregation function on the StormComponent(Bolt) where join is performed

		//1 - discount
		ValueExpression<Double> substract = new Subtraction(
				new ValueSpecification(_doubleConv, 1.0), new ColumnReference(_doubleConv, 5));
		//extendedPrice*(1-discount)
		ValueExpression<Double> product = new Multiplication(new ColumnReference(_doubleConv, 4),
				substract);
		AggregateOperator agg = new AggregateSumOperator(product, conf).setGroupByColumns(Arrays
				.asList(0, 1, 2));

		ColumnReference colC_O = new ColumnReference(_ic, 0);
		ColumnReference colL = new ColumnReference(_ic, 0);
		ComparisonPredicate C_O_L_comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
				colC_O, colL);

		Component C_O_Ljoin = ThetaJoinComponentFactory
				.createThetaJoinOperator(Theta_JoinType, C_Ojoin, relationLineitem, _queryBuilder)
				.add(agg).setJoinPredicate(C_O_L_comp).setContentSensitiveThetaJoinWrapper(_ic);

		//-------------------------------------------------------------------------------------

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
