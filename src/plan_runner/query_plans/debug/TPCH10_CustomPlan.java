package plan_runner.query_plans.debug;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.conversion.DateConversion;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.DateSum;
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

public class TPCH10_CustomPlan {
    private static Logger LOG = Logger.getLogger(TPCH10_CustomPlan.class);

    private static final TypeConversion<Date> _dc = new DateConversion();
    private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
    private static final StringConversion _sc = new StringConversion();
    private QueryBuilder _queryBuilder = new QueryBuilder();

    //query variables
    private static Date _date1, _date2;

    private static void computeDates(){
        // date2= date1 + 3 months
//      String date1Str = "1993-10-01";
//        String date1Str = "1996-01-02";
    	String date1Str = "1996-12-01";
        int interval = 3;
        int unit = Calendar.MONTH;

        //setting _date1
        _date1 = _dc.fromString(date1Str);

        //setting _date2
        ValueExpression<Date> date1Ve, date2Ve;
        date1Ve = new ValueSpecification<Date>(_dc, _date1);
        date2Ve = new DateSum(date1Ve, unit, interval);
        _date2 = date2Ve.eval(null);
        // tuple is set to null since we are computing based on constants
    }

    public TPCH10_CustomPlan(String dataPath, String extension, Map conf){
        computeDates();

        //-------------------------------------------------------------------------------------
        List<Integer> hashCustomer = Arrays.asList(0);

        ProjectOperator projectionCustomer = new ProjectOperator(new int[]{0, 1, 2, 3, 4, 5, 7});

        DataSourceComponent relationCustomer = new DataSourceComponent(
                "CUSTOMER",
                dataPath + "customer" + extension).setOutputPartKey(hashCustomer)
                           .add(projectionCustomer);
        _queryBuilder.add(relationCustomer);

        //-------------------------------------------------------------------------------------
        List<Integer> hashOrders = Arrays.asList(1);

        SelectOperator selectionOrders = new SelectOperator(
                new BetweenPredicate(
                    new ColumnReference(_dc, 4),
                    true, new ValueSpecification(_dc, _date1),
                    false, new ValueSpecification(_dc, _date2)
                ));

        ProjectOperator projectionOrders = new ProjectOperator(new int[]{0, 1});

        DataSourceComponent relationOrders = new DataSourceComponent(
                "ORDERS",
                dataPath + "orders" + extension).setOutputPartKey(hashOrders)
                           .add(selectionOrders)
                           .add(projectionOrders);
        _queryBuilder.add(relationOrders);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent C_Ojoin = new EquiJoinComponent(
				relationCustomer,
				relationOrders).setOutputPartKey(Arrays.asList(3));
        _queryBuilder.add(C_Ojoin);
        //-------------------------------------------------------------------------------------
        List<Integer> hashNation = Arrays.asList(0);

        ProjectOperator projectionNation = new ProjectOperator(new int[]{0, 1});

        DataSourceComponent relationNation = new DataSourceComponent(
                "NATION", dataPath + "nation" + extension)
        			.setOutputPartKey(hashNation)
                    .add(projectionNation);
        _queryBuilder.add(relationNation);
        //-------------------------------------------------------------------------------------

        EquiJoinComponent C_O_Njoin = new EquiJoinComponent(C_Ojoin, relationNation)
        		.add(new ProjectOperator(new int[]{0, 1, 2, 4, 5, 6, 7, 8}))
                .setOutputPartKey(Arrays.asList(6));
        _queryBuilder.add(C_O_Njoin);
        //-------------------------------------------------------------------------------------

        List<Integer> hashLineitem = Arrays.asList(0);

        SelectOperator selectionLineitem = new SelectOperator(
                new ComparisonPredicate(
                    new ColumnReference(_sc, 8),
                    new ValueSpecification(_sc, "N")
                ));

        ProjectOperator projectionLineitem = new ProjectOperator(new int[]{0, 5, 6});

        DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM", dataPath + "lineitem" + extension)
        			.setOutputPartKey(hashLineitem)
                    .add(selectionLineitem)
                    .add(projectionLineitem);
        _queryBuilder.add(relationLineitem);

        //-------------------------------------------------------------------------------------
        // set up aggregation function on the StormComponent(Bolt) where join is performed

	//1 - discount
	ValueExpression<Double> substract = new Subtraction(
			new ValueSpecification(_doubleConv, 1.0),
			new ColumnReference(_doubleConv, 8));
	//extendedPrice*(1-discount)
	ValueExpression<Double> product = new Multiplication(
			new ColumnReference(_doubleConv, 7),
			substract);
	AggregateOperator agg = new AggregateSumOperator(product, conf)
		.setGroupByColumns(Arrays.asList(0, 1, 4, 6, 2, 3, 5));

        EquiJoinComponent C_O_N_Ljoin = new EquiJoinComponent(
				C_O_Njoin, relationLineitem)
        	.add(new ProjectOperator(new int[]{0, 1, 2, 3, 4, 5, 7, 8, 9}))
            .add(agg);
        _queryBuilder.add(C_O_N_Ljoin);
        //-------------------------------------------------------------------------------------

    }

    public QueryBuilder getQueryPlan() {
        return _queryBuilder;
    }
}
